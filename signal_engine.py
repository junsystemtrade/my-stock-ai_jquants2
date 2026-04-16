"""
signal_engine.py
================
テクニカルシグナルの計算と、スコアリングに必要な環境指標の付与を担当。

変更点:
  - scan_signals() の上場廃止フィルターを強化
    JPXマスターに存在しない銘柄を確実に除外
    かつ直近5営業日データがない銘柄も除外（廃止後DBに残るデータ対策）
  - check_exit_signals() で entry_price=NULL のポジションをスキップ
"""

import os
import yaml
import pandas as pd
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from sqlalchemy import text
import database_manager
import portfolio_manager

_CONFIG_PATH = Path(__file__).parent / "signals_config.yml"


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {
        "signals": {
            "golden_cross": {"enabled": True, "short_window": 5, "long_window": 25},
            "rsi_oversold":  {"enabled": True, "window": 14, "threshold": 50},
            "volume_surge":  {"enabled": True, "window": 20, "multiplier": 2.0},
        },
        "filter": {
            "min_price": 500, "max_price": 50000,
            "min_data_days": 80, "min_daily_turnover_avg_20": 500000000,
            "exclude_code_range": [[1000, 1999]], "max_signals_per_ticker": 1,
        },
        "exit_rules": {
            "immediate": {"dead_cross": True, "rsi_overbought": 70},
            "hold_days": 10,
            "trailing": {"enabled": True, "conditions": {
                "golden_cross_maintained": True, "rsi_below": 70, "profit_required": True,
            }}
        }
    }


def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


# -----------------------------------------------------------------------
# 地合い判定
# -----------------------------------------------------------------------
def _get_market_condition() -> tuple[float, str]:
    db    = database_manager.DBManager()
    query = text("SELECT price FROM daily_prices WHERE ticker = 'NIY=F' ORDER BY date DESC LIMIT 2")
    try:
        with db.engine.connect() as conn:
            df_niy = pd.read_sql(query, conn)
        if len(df_niy) < 2:
            return 0.0, "不明"
        p_now    = float(df_niy["price"].iloc[0])
        p_prev   = float(df_niy["price"].iloc[1])
        m_change = (p_now - p_prev) / p_prev * 100
        if m_change <= -2.0:    status = "暴落警戒"
        elif m_change <= -0.5:  status = "軟調"
        elif m_change >= 0.5:   status = "好調"
        else:                   status = "平穏"
        return round(m_change, 2), status
    except Exception as e:
        print(f"WARNING market condition: {e}")
        return 0.0, "エラー"


# -----------------------------------------------------------------------
# ストップ高判定
# -----------------------------------------------------------------------
def _is_stop_high(price: float, prev_price: float) -> bool:
    if prev_price <= 0:
        return False
    if prev_price < 100:       limit = 30
    elif prev_price < 200:     limit = 50
    elif prev_price < 500:     limit = 80
    elif prev_price < 700:     limit = 100
    elif prev_price < 1000:    limit = 150
    elif prev_price < 1500:    limit = 300
    elif prev_price < 2000:    limit = 400
    elif prev_price < 3000:    limit = 500
    elif prev_price < 5000:    limit = 700
    elif prev_price < 7000:    limit = 1000
    elif prev_price < 10000:   limit = 1500
    elif prev_price < 15000:   limit = 3000
    elif prev_price < 20000:   limit = 4000
    elif prev_price < 30000:   limit = 5000
    elif prev_price < 50000:   limit = 7000
    elif prev_price < 70000:   limit = 10000
    elif prev_price < 100000:  limit = 15000
    elif prev_price < 150000:  limit = 30000
    elif prev_price < 200000:  limit = 40000
    elif prev_price < 300000:  limit = 50000
    elif prev_price < 500000:  limit = 70000
    elif prev_price < 700000:  limit = 100000
    elif prev_price < 1000000: limit = 150000
    else:                      limit = 200000
    return (price - prev_price) >= limit


# -----------------------------------------------------------------------
# 指標計算
# -----------------------------------------------------------------------
def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df     = df.copy()
    close  = df["price"].astype(float)
    volume = df["volume"].astype(float)

    df["volume_ratio"]    = volume / volume.rolling(window=5).mean().shift(1)
    ma25                  = close.rolling(window=25).mean()
    df["mavg_25_diff"]    = (close - ma25) / ma25 * 100
    ma5                   = close.rolling(window=5).mean()
    df["mavg_5_diff"]     = (close - ma5) / ma5 * 100
    df["sma_5"]           = ma5
    df["sma_25"]          = ma25
    df["turnover"]        = close * volume
    df["turnover_avg_20"] = df["turnover"].rolling(window=20).mean()
    df["is_above_ma25"]   = close > ma25
    df["ma25_upward"]     = ma25.diff() > 0

    delta = close.diff()
    gain  = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss  = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df["rsi_14"] = 100 - (100 / (1 + (gain / loss.replace(0, float("nan")))))
    return df


# -----------------------------------------------------------------------
# シグナル判定コア
# -----------------------------------------------------------------------
def _check_signals(ticker: str, df: pd.DataFrame, cfg: dict) -> list[dict]:
    code_str = ticker.replace(".T", "").strip()
    if any(
        r[0] <= int(code_str) <= r[1]
        for r in cfg["filter"].get("exclude_code_range", [])
        if code_str.isdigit()
    ):
        return []

    df       = _calculate_indicators(df)
    min_days = cfg["filter"].get("min_data_days", 80)
    if len(df) < min_days:
        return []

    row      = df.iloc[-1]
    prev_row = df.iloc[-2] if len(df) >= 2 else None
    price    = float(row["price"])

    stop_high_cfg = cfg.get("filter", {}).get("stop_high", {})
    if stop_high_cfg.get("enabled", True) and prev_row is not None:
        if _is_stop_high(price, float(prev_row["price"])):
            return []

    min_turnover = cfg["filter"].get("min_daily_turnover_avg_20", 500000000)
    if row["turnover_avg_20"] < min_turnover:
        return []
    if not (cfg["filter"]["min_price"] <= price <= cfg["filter"]["max_price"]):
        return []

    res   = []
    sma_s = df["price"].rolling(window=5).mean()
    sma_l = df["price"].rolling(window=25).mean()
    if len(sma_s) > 1 and sma_s.iloc[-2] <= sma_l.iloc[-2] and sma_s.iloc[-1] > sma_l.iloc[-1]:
        res.append({"signal_type": "ゴールデンクロス(短期)", "reason": "5日線が25日線を上抜け", "priority": 1})

    if row["rsi_14"] < cfg["signals"]["rsi_oversold"]["threshold"]:
        res.append({"signal_type": "RSI中立以下", "reason": f"RSI: {row['rsi_14']:.1f}", "priority": 2})

    if row["volume_ratio"] >= cfg["signals"]["volume_surge"]["multiplier"]:
        res.append({"signal_type": "出来高急増", "reason": f"出来高 {row['volume_ratio']:.1f}倍", "priority": 3})

    if not res:
        return []

    best = sorted(res, key=lambda x: x["priority"])[0]
    best.update({"ticker": ticker, "price": price, **row.to_dict()})
    return [best]


# -----------------------------------------------------------------------
# 手じまいシグナル判定
# -----------------------------------------------------------------------
def check_exit_signals(daily_data: pd.DataFrame) -> list[dict]:
    cfg            = _load_config()
    exit_cfg       = cfg.get("exit_rules", {})
    immediate_cfg  = exit_cfg.get("immediate", {})
    hold_days      = exit_cfg.get("hold_days", 10)
    trailing_cfg   = exit_cfg.get("trailing", {})
    trailing_on    = trailing_cfg.get("enabled", True)
    trail_cond     = trailing_cfg.get("conditions", {})
    rsi_overbought = immediate_cfg.get("rsi_overbought", 70)

    db             = database_manager.DBManager()
    open_positions = db.load_open_positions()
    if open_positions.empty:
        print("INFO: no open positions")
        return []

    today        = daily_data["date"].max()
    exit_signals = []

    for _, pos in open_positions.iterrows():
        ticker      = pos["ticker"]
        entry_date  = pd.to_datetime(pos["entry_date"]).date()
        entry_price = pos["entry_price"]

        # entry_price が NULL（未確定）のポジションはスキップ
        if entry_price is None or pd.isna(entry_price):
            print(f"SKIP {ticker}: entry_price not set yet")
            continue

        entry_price = float(entry_price)
        df_ticker   = daily_data[daily_data["ticker"] == ticker].sort_values("date")
        if df_ticker.empty or len(df_ticker) < 26:
            continue

        df_ticker     = _calculate_indicators(df_ticker)
        row           = df_ticker.iloc[-1]
        current_price = float(row["price"])
        pnl_pct       = (current_price - entry_price) / entry_price * 100
        held_days     = (pd.to_datetime(today).date() - entry_date).days

        sma_s       = df_ticker["sma_5"]
        sma_l       = df_ticker["sma_25"]
        is_gc       = sma_s.iloc[-1] > sma_l.iloc[-1]
        is_dc       = len(sma_s) > 1 and sma_s.iloc[-2] >= sma_l.iloc[-2] and sma_s.iloc[-1] < sma_l.iloc[-1]
        rsi         = float(row["rsi_14"])
        exit_reason = None

        if held_days < hold_days:
            if immediate_cfg.get("dead_cross", True) and is_dc:
                exit_reason = "デッドクロス発生"
            elif rsi >= rsi_overbought:
                exit_reason = f"RSI過熱 ({rsi:.1f})"
        else:
            if trailing_on:
                reasons = []
                if trail_cond.get("golden_cross_maintained", True) and not is_gc:
                    reasons.append("5日線<25日線")
                if rsi >= trail_cond.get("rsi_below", 70):
                    reasons.append(f"RSI過熱({rsi:.1f})")
                if trail_cond.get("profit_required", True) and pnl_pct <= 0:
                    reasons.append("含み損転落")
                if reasons:
                    exit_reason = f"トレーリング手じまい（{' / '.join(reasons)}）"
                else:
                    print(f"OK {ticker}: hold ({held_days}days / {pnl_pct:+.2f}%)")
            else:
                exit_reason = f"保有{held_days}日経過"

        if exit_reason:
            exit_signals.append({
                "ticker":        ticker,
                "entry_date":    entry_date,
                "entry_price":   entry_price,
                "current_price": current_price,
                "pnl_pct":       round(pnl_pct, 2),
                "exit_reason":   exit_reason,
                "held_days":     held_days,
            })
            db.close_position(
                ticker=ticker, entry_date=entry_date,
                close_reason=exit_reason, closed_date=today, exit_price=current_price,
            )

    print(f"INFO exit signals: {len(exit_signals)}")
    return exit_signals


# -----------------------------------------------------------------------
# メインスキャン（買いシグナル）
# -----------------------------------------------------------------------
def scan_signals(daily_data: pd.DataFrame, market_status: str = None) -> list[dict]:
    if daily_data.empty:
        return []

    cfg = _load_config()

    market_change, market_status_actual = _get_market_condition()
    if market_status is None:
        market_status = market_status_actual

    breaker_cfg = cfg.get("filter", {}).get("market_breaker", {})
    if breaker_cfg.get("enabled", False):
        threshold = breaker_cfg.get("drop_threshold_pct", -1.5)
        if market_change <= threshold:
            print(f"WARNING market_breaker: {market_change:+.2f}% -> skip scan")
            return []

    # ---------------------------------------------------------------
    # フィルター① JPXマスターに存在する銘柄のみ（上場廃止・整理・監理除外）
    # ---------------------------------------------------------------
    jpx_tickers = set()
    try:
        jpx_tickers = set(portfolio_manager.get_target_tickers().keys())
        print(f"INFO JPX tickers: {len(jpx_tickers)}")
    except Exception as e:
        print(f"WARNING JPX fetch failed: {e}")

    # ---------------------------------------------------------------
    # フィルター② daily_data の最終日を確認して古い銘柄を除外
    # 「DBの最大日付から5日以上離れている銘柄」= 上場廃止等でデータが途切れた銘柄
    # ---------------------------------------------------------------
    db_max_date = daily_data[daily_data["ticker"] != "NIY=F"]["date"].max()
    stale_limit = pd.Timestamp(db_max_date) - pd.Timedelta(days=7)

    latest_per_ticker = (
        daily_data[daily_data["ticker"] != "NIY=F"]
        .groupby("ticker")["date"]
        .max()
    )
    # DB全体の最終日から7日以上古い銘柄 = 上場廃止等とみなして除外
    stale_tickers = set(latest_per_ticker[latest_per_ticker < stale_limit.date()].index)
    if stale_tickers:
        print(f"INFO stale tickers excluded (possibly delisted): {len(stale_tickers)}")

    # ---------------------------------------------------------------
    # フィルター③ オープンポジションがある銘柄を除外
    # ---------------------------------------------------------------
    open_positions = database_manager.DBManager().load_open_positions()
    open_tickers   = set(open_positions["ticker"].tolist()) if not open_positions.empty else set()
    if open_tickers:
        print(f"INFO open position tickers excluded: {open_tickers}")

    all_hits = []
    for ticker, df_ticker in daily_data[daily_data["ticker"] != "NIY=F"].groupby("ticker"):

        # ① JPXマスター除外
        if jpx_tickers and ticker not in jpx_tickers:
            continue

        # ② 古データ除外（上場廃止銘柄がDBに残っているケース）
        if ticker in stale_tickers:
            continue

        # ③ オープンポジション除外
        if ticker in open_tickers:
            continue

        df_sorted = df_ticker.sort_values("date")
        hits      = _check_signals(ticker, df_sorted, cfg)
        for h in hits:
            h["market_status"] = market_status
            all_hits.append(h)

    return all_hits
