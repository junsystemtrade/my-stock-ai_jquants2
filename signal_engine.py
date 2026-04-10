"""
signal_engine.py
================
テクニカルシグナルの計算と、スコアリングに必要な環境指標の付与を担当。
手じまいシグナル判定機能を追加。
"""

import os
import yaml
import pandas as pd
from datetime import date
from pathlib import Path
from sqlalchemy import text
import database_manager

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

        p_now, p_prev = float(df_niy['price'].iloc[0]), float(df_niy['price'].iloc[1])
        m_change = (p_now - p_prev) / p_prev * 100
        
        if m_change <= -2.0: status = "暴落警戒"
        elif m_change <= -0.5: status = "軟調"
        elif m_change >= 0.5: status = "好調"
        else: status = "平穏"
            
        return round(m_change, 2), status
    except Exception as e:
        print(f"⚠️ 地合い判定失敗: {e}")
        return 0.0, "エラー"

# -----------------------------------------------------------------------
# 指標計算（ベクトル演算で高速化）
# -----------------------------------------------------------------------
def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close = df["price"].astype(float)
    volume = df["volume"].astype(float)
    
    df['volume_ratio'] = volume / volume.rolling(window=5).mean().shift(1)
    
    ma25 = close.rolling(window=25).mean()
    df['mavg_25_diff'] = (close - ma25) / ma25 * 100

    ma5 = close.rolling(window=5).mean()
    df['mavg_5_diff'] = (close - ma5) / ma5 * 100
    df['sma_5'] = ma5
    df['sma_25'] = ma25

    df['turnover'] = close * volume
    df['turnover_avg_20'] = df['turnover'].rolling(window=20).mean()

    df['is_above_ma25'] = close > ma25
    df['ma25_upward'] = ma25.diff() > 0

    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi_14'] = 100 - (100 / (1 + (gain / loss.replace(0, float("nan")))))

    return df

# -----------------------------------------------------------------------
# シグナル判定コア（買い）
# -----------------------------------------------------------------------
def _check_signals(ticker: str, df: pd.DataFrame, cfg: dict) -> list[dict]:
    code_str = ticker.replace(".T", "").strip()
    if any(r[0] <= int(code_str) <= r[1] for r in cfg["filter"].get("exclude_code_range", []) if code_str.isdigit()):
        return []

    df = _calculate_indicators(df)
    
    min_days = cfg["filter"].get("min_data_days", 80)
    if len(df) < min_days: return []
    
    row = df.iloc[-1]
    price = float(row["price"])
    
    min_turnover = cfg["filter"].get("min_daily_turnover_avg_20", 500000000)
    if row["turnover_avg_20"] < min_turnover: return []
    if not (cfg["filter"]["min_price"] <= price <= cfg["filter"]["max_price"]): return []

    res = []
    sma_s = df["price"].rolling(window=5).mean()
    sma_l = df["price"].rolling(window=25).mean()
    if len(sma_s) > 1 and sma_s.iloc[-2] <= sma_l.iloc[-2] and sma_s.iloc[-1] > sma_l.iloc[-1]:
        res.append({"signal_type": "ゴールデンクロス(短期)", "reason": "5日線が25日線を上抜け", "priority": 1})

    if row["rsi_14"] < cfg["signals"]["rsi_oversold"]["threshold"]:
        res.append({"signal_type": "RSI中立以下", "reason": f"RSI: {row['rsi_14']:.1f}", "priority": 2})

    if row["volume_ratio"] >= cfg["signals"]["volume_surge"]["multiplier"]:
        res.append({"signal_type": "出来高急増", "reason": f"出来高 {row['volume_ratio']:.1f}倍", "priority": 3})

    if not res: return []

    best_signal = sorted(res, key=lambda x: x["priority"])[0]
    best_signal.update({
        "ticker": ticker, 
        "price": price, 
        **row.to_dict()
    })
    return [best_signal]

# -----------------------------------------------------------------------
# 手じまいシグナル判定
# -----------------------------------------------------------------------
def check_exit_signals(daily_data: pd.DataFrame, hold_days: int = 10) -> list[dict]:
    """
    オープンポジションに対して手じまい条件を判定する。
    条件1（テクニカル）: デッドクロス（5日線が25日線を下抜け）またはRSI70超え
    条件2（保有日数）  : entry_dateから hold_days 日以上経過
    """
    db = database_manager.DBManager()
    open_positions = db.load_open_positions()
    if open_positions.empty:
        print("📂 オープンポジションなし")
        return []

    today = daily_data["date"].max()
    exit_signals = []

    for _, pos in open_positions.iterrows():
        ticker = pos["ticker"]
        entry_date = pd.to_datetime(pos["entry_date"]).date()
        entry_price = float(pos["entry_price"])

        # 該当銘柄のデータを取得
        df_ticker = daily_data[daily_data["ticker"] == ticker].sort_values("date")
        if df_ticker.empty or len(df_ticker) < 26:
            continue

        df_ticker = _calculate_indicators(df_ticker)
        row = df_ticker.iloc[-1]
        current_price = float(row["price"])
        pnl_pct = (current_price - entry_price) / entry_price * 100

        exit_reason = None

        # 条件1: テクニカル手じまい
        sma_s = df_ticker["sma_5"]
        sma_l = df_ticker["sma_25"]
        # デッドクロス（5日線が25日線を下抜け）
        if len(sma_s) > 1 and sma_s.iloc[-2] >= sma_l.iloc[-2] and sma_s.iloc[-1] < sma_l.iloc[-1]:
            exit_reason = "💀 デッドクロス発生"
        # RSI過熱
        elif row["rsi_14"] >= 70:
            exit_reason = f"🌡️ RSI過熱 ({row['rsi_14']:.1f})"

        # 条件2: 保有日数超過
        if exit_reason is None:
            held_days = (pd.to_datetime(today).date() - entry_date).days
            if held_days >= hold_days:
                exit_reason = f"⏰ 保有{held_days}日経過"

        if exit_reason:
            exit_signals.append({
                "ticker": ticker,
                "entry_date": entry_date,
                "entry_price": entry_price,
                "current_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "exit_reason": exit_reason,
            })
            # DBのポジションをクローズ
            db.close_position(
                ticker=ticker,
                entry_date=entry_date,
                close_reason=exit_reason,
                closed_date=today,
            )

    print(f"🚨 手じまいシグナル: {len(exit_signals)} 件")
    return exit_signals

# -----------------------------------------------------------------------
# メインスキャン（買いシグナル）
# -----------------------------------------------------------------------
def scan_signals(daily_data: pd.DataFrame, market_status: str = None) -> list[dict]:
    if daily_data.empty: return []
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

    all_hits = []
    for ticker, df_ticker in daily_data[daily_data["ticker"] != "NIY=F"].groupby("ticker"):
        df_sorted = df_ticker.sort_values("date")
        hits = _check_signals(ticker, df_sorted, cfg)
        for h in hits:
            h["market_status"] = market_status
            all_hits.append(h)

    return all_hits
