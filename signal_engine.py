"""
signal_engine.py
================
テクニカルシグナルの計算と、スコアリングに必要な環境指標の付与を担当。
手じまいシグナル判定・トレーリング・ストップ高除外機能を追加。
JPXマスター除外・オープンポジション除外を追加。
"""

import os
import yaml
import pandas as pd
from datetime import date
from pathlib import Path
from sqlalchemy import text
import database_manager
import portfolio_manager

# -----------------------------------------------------------------------
# 設定の読み込み
# -----------------------------------------------------------------------
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
            "min_price": 500,
            "max_price": 50000,
            "min_data_days": 80,
            "min_daily_turnover_avg_20": 500000000,
            "exclude_code_range": [[1000, 1999]],
            "max_signals_per_ticker": 1,
        },
        "exit_rules": {
            "immediate": {"dead_cross": True, "rsi_overbought": 70},
            "hold_days": 10,
            "trailing": {
                "enabled": True,
                "conditions": {
                    "golden_cross_maintained": True,
                    "rsi_below": 70,
                    "profit_required": True,
                }
            }
        }
    }

# -----------------------------------------------------------------------
# 地合い判定
# -----------------------------------------------------------------------
def _get_market_condition() -> tuple[float, str]:
    """日経平均先物(NIY=F)の直近比較で地合いを判定"""
    db = database_manager.DBManager()
    query = text("""
        SELECT price FROM daily_prices 
        WHERE ticker = 'NIY=F' 
        ORDER BY date DESC LIMIT 2
    """)
    try:
        with db.engine.connect() as conn:
            df_niy = pd.read_sql(query, conn)
        if len(df_niy) < 2:
            return 0.0, "不明"
        p_now  = float(df_niy['price'].iloc[0])
        p_prev = float(df_niy['price'].iloc[1])
        m_change = (p_now - p_prev) / p_prev * 100
        if m_change <= -2.0:   status = "暴落警戒"
        elif m_change <= -0.5: status = "軟調"
        elif m_change >= 0.5:  status = "好調"
        else:                  status = "平穏"
        return round(m_change, 2), status
    except Exception as e:
        print(f"⚠️ 地合い判定失敗: {e}")
        return 0.0, "エラー"

# -----------------------------------------------------------------------
# ストップ高判定
# -----------------------------------------------------------------------
def _is_stop_high(price: float, prev_price: float) -> bool:
    """東証ルールに基づきストップ高に達しているか判定する"""
    if prev_price <= 0:
        return False
    if prev_price < 100:        limit = 30
    elif prev_price < 200:      limit = 50
    elif prev_price < 500:      limit = 80
    elif prev_price < 700:      limit = 100
    elif prev_price < 1000:     limit = 150
    elif prev_price < 1500:     limit = 300
    elif prev_price < 2000:     limit = 400
    elif prev_price < 3000:     limit = 500
    elif prev_price < 5000:     limit = 700
    elif prev_price < 7000:     limit = 1000
    elif prev_price < 10000:    limit = 1500
    elif prev_price < 15000:    limit = 3000
    elif prev_price < 20000:    limit = 4000
    elif prev_price < 30000:    limit = 5000
    elif prev_price < 50000:    limit = 7000
    elif prev_price < 70000:    limit = 10000
    elif prev_price < 100000:   limit = 15000
    elif prev_price < 150000:   limit = 30000
    elif prev_price < 200000:   limit = 40000
    elif prev_price < 300000:   limit = 50000
    elif prev_price < 500000:   limit = 70000
    elif prev_price < 700000:   limit = 100000
    elif prev_price < 1000000:  limit = 150000
    else:                       limit = 200000
    return (price - prev_price) >= limit

# -----------------------------------------------------------------------
# 指標計算（ベクトル演算で高速化）
# -----------------------------------------------------------------------
def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close  = df["price"].astype(float)
    volume = df["volume"].astype(float)

    df['volume_ratio']    = volume / volume.rolling(window=5).mean().shift(1)

    ma25 = close.rolling(window=25).mean()
    df['mavg_25_diff']    = (close - ma25) / ma25 * 100

    ma5 = close.rolling(window=5).mean()
    df['mavg_5_diff']     = (close - ma5) / ma5 * 100
    df['sma_5']           = ma5
    df['sma_25']          = ma25

    df['turnover']        = close * volume
    df['turnover_avg_20'] = df['turnover'].rolling(window=20).mean()

    df['is_above_ma25']   = close > ma25
    df['ma25_upward']     = ma25.diff() > 0

    delta = close.diff()
    gain  = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss  = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi_14'] = 100 - (100 / (1 + (gain / loss.replace(0, float("nan")))))

    return df

# -----------------------------------------------------------------------
# シグナル判定コア（買い）
# -----------------------------------------------------------------------
def _check_signals(ticker: str, df: pd.DataFrame, cfg: dict) -> list[dict]:
    code_str = ticker.replace(".T", "").strip()
    if any(
        r[0] <= int(code_str) <= r[1]
        for r in cfg["filter"].get("exclude_code_range", [])
        if code_str.isdigit()
    ):
        return []

    df = _calculate_indicators(df)

    min_days = cfg["filter"].get("min_data_days", 80)
    if len(df) < min_days:
        return []

    row      = df.iloc[-1]
    prev_row = df.iloc[-2] if len(df) >= 2 else None
    price    = float(row["price"])

    # ストップ高チェック
    stop_high_cfg = cfg.get("filter", {}).get("stop_high", {})
    if stop_high_cfg.get("enabled", True) and prev_row is not None:
        if _is_stop_high(price, float(prev_row["price"])):
            print(f"⛔ {ticker}: ストップ高のためスキャン除外")
            return []

    # フィルタ判定
    min_turnover = cfg["filter"].get("min_daily_turnover_avg_20", 500000000)
    if row["turnover_avg_20"] < min_turnover:
        return []
    if not (cfg["filter"]["min_price"] <= price <= cfg["filter"]["max_price"]):
        return []

    res = []
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

    best_signal = sorted(res, key=lambda x: x["priority"])[0]
    best_signal.update({
        "ticker": ticker,
        "price":  price,
        **row.to_dict()
    })
    return [best_signal]

# -----------------------------------------------------------------------
# 手じまいシグナル判定（トレーリング対応）
# -----------------------------------------------------------------------
def check_exit_signals(daily_data: pd.DataFrame) -> list[dict]:
    """
    オープンポジションに対して手じまい条件を判定する。

    【10日未満】
      即時手じまい条件:
        - デッドクロス（5日線が25日線を下抜け）
        - RSI70超え

    【10日以降】毎日トレーリング判定:
      以下の全条件を満たす場合のみ保有継続、1つでも外れたら手じまい
        - ①5日線 > 25日線（ゴールデンクロス維持）
        - ②RSI < 70（過熱感なし）
        - ③含み益がプラス
    """
    cfg = _load_config()
    exit_cfg       = cfg.get("exit_rules", {})
    immediate_cfg  = exit_cfg.get("immediate", {})
    hold_days      = exit_cfg.get("hold_days", 10)
    trailing_cfg   = exit_cfg.get("trailing", {})
    trailing_on    = trailing_cfg.get("enabled", True)
    trail_cond     = trailing_cfg.get("conditions", {})
    rsi_overbought = immediate_cfg.get("rsi_overbought", 70)

    db = database_manager.DBManager()
    open_positions = db.load_open_positions()
    if open_positions.empty:
        print("📂 オープンポジションなし")
        return []

    today        = daily_data["date"].max()
    exit_signals = []

    for _, pos in open_positions.iterrows():
        ticker      = pos["ticker"]
        entry_date  = pd.to_datetime(pos["entry_date"]).date()
        entry_price = float(pos["entry_price"])

        df_ticker = daily_data[daily_data["ticker"] == ticker].sort_values("date")
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
            # ─── 10日未満: 即時手じまい条件のみチェック ───
            if immediate_cfg.get("dead_cross", True) and is_dc:
                exit_reason = "💀 デッドクロス発生"
            elif rsi >= rsi_overbought:
                exit_reason = f"🌡️ RSI過熱 ({rsi:.1f})"
        else:
            # ─── 10日以降: トレーリング判定（毎日） ───
            if trailing_on:
                reasons = []
                if trail_cond.get("golden_cross_maintained", True) and not is_gc:
                    reasons.append("5日線<25日線")
                if rsi >= trail_cond.get("rsi_below", 70):
                    reasons.append(f"RSI過熱({rsi:.1f})")
                if trail_cond.get("profit_required", True) and pnl_pct <= 0:
                    reasons.append("含み損転落")
                if reasons:
                    exit_reason = f"📉 トレーリング手じまい（{' / '.join(reasons)}）"
                else:
                    print(f"✅ {ticker}: 保有継続（{held_days}日目 / 含み益{pnl_pct:+.2f}%）")
            else:
                exit_reason = f"⏰ 保有{held_days}日経過（期間満了）"

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
                ticker=ticker,
                entry_date=entry_date,
                close_reason=exit_reason,
                closed_date=today,
                exit_price=current_price,
            )

    print(f"🚨 手じまいシグナル: {len(exit_signals)} 件")
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
            print(f"🚨 market_breaker 発動: 日経先物 {market_change:+.2f}% → 本日のスキャンをスキップします")
            return []

    # ★ フィルター1: JPXマスターに存在する銘柄のみを対象にする
    jpx_tickers = None
    try:
        jpx_tickers = set(portfolio_manager.get_target_tickers().keys())
        print(f"📋 JPXマスター有効銘柄数: {len(jpx_tickers)}")
    except Exception as e:
        print(f"⚠️ JPXマスター取得失敗（フィルタースキップ）: {e}")

    # ★ フィルター2: オープンポジションがある銘柄を除外
    db = database_manager.DBManager()
    open_positions = db.load_open_positions()
    open_tickers = set(open_positions["ticker"].tolist()) if not open_positions.empty else set()
    if open_tickers:
        print(f"📂 オープンポジション除外: {open_tickers}")

    all_hits = []
    for ticker, df_ticker in daily_data[daily_data["ticker"] != "NIY=F"].groupby("ticker"):

        # JPXマスター除外チェック（上場廃止・整理・監理ポスト）
        if jpx_tickers is not None and ticker not in jpx_tickers:
            continue

        # オープンポジション除外チェック（同じ銘柄の重複シグナル防止）
        if ticker in open_tickers:
            continue

        df_sorted = df_ticker.sort_values("date")
        hits = _check_signals(ticker, df_sorted, cfg)
        for h in hits:
            h["market_status"] = market_status
            all_hits.append(h)

    return all_hits
