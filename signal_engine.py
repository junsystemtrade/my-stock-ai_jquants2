"""
signal_engine.py
================
テクニカルシグナルの計算と、スコアリングに必要な環境指標の付与を担当。
"""

import os
import yaml
import pandas as pd
from pathlib import Path
from sqlalchemy import text
import database_manager

# -----------------------------------------------------------------------
# 設定の読み込み
# -----------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent / "signals_config.yml"

# signal_engine.py の _load_config() 内、デフォルト設定を修正
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
            "min_data_days": 80,  # ← 150 から 80 に修正（YAMLと統一）
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
            return 0.0, "不明" # 指標②：バックフィル未完了時は「不明」

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
    
    # 出来高比（5日平均に対して）
    df['volume_ratio'] = volume / volume.rolling(window=5).mean().shift(1)
    
    # 25日線乖離率（スコア計算の要）
    ma25 = close.rolling(window=25).mean()
    df['mavg_25_diff'] = (close - ma25) / ma25 * 100

    # 5日線乖離率
    ma5 = close.rolling(window=5).mean()
    df['mavg_5_diff'] = (close - ma5) / ma5 * 100

    # 売買代金（ターンオーバー）の20日平均 - 指標①
    df['turnover'] = close * volume
    df['turnover_avg_20'] = df['turnover'].rolling(window=20).mean()

    # トレンド判定
    df['is_above_ma25'] = close > ma25
    df['ma25_upward'] = ma25.diff() > 0

    # RSI (14日)
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi_14'] = 100 - (100 / (1 + (gain / loss.replace(0, float("nan")))))

    return df

# -----------------------------------------------------------------------
# シグナル判定コア
# -----------------------------------------------------------------------
def _check_signals(ticker: str, df: pd.DataFrame, cfg: dict) -> list[dict]:
    # 除外チェック
    code_str = ticker.replace(".T", "").strip()
    if any(r[0] <= int(code_str) <= r[1] for r in cfg["filter"].get("exclude_code_range", []) if code_str.isdigit()):
        return []

    # 指標計算
    df = _calculate_indicators(df)
    
    # 指標③：十分な計算期間を確保（150日）
    min_days = cfg["filter"].get("min_data_days", 150)
    if len(df) < min_days: return []
    
    row = df.iloc[-1]
    price = float(row["price"])
    
    # フィルタ判定（価格帯 ＋ 指標① 売買代金）
    min_turnover = cfg["filter"].get("min_daily_turnover_avg_20", 500000000)
    if row["turnover_avg_20"] < min_turnover: return []
    if not (cfg["filter"]["min_price"] <= price <= cfg["filter"]["max_price"]): return []

    res = []
    # ① ゴールデンクロス (短期 5/25) - 指標⑤
    sma_s = df["price"].rolling(window=5).mean()
    sma_l = df["price"].rolling(window=25).mean()
    if len(sma_s) > 1 and sma_s.iloc[-2] <= sma_l.iloc[-2] and sma_s.iloc[-1] > sma_l.iloc[-1]:
        res.append({"signal_type": "ゴールデンクロス(短期)", "reason": "5日線が25日線を上抜け", "priority": 1})

    # ② RSI中立以下 (名称変更) - 指標⑥
    if row["rsi_14"] < cfg["signals"]["rsi_oversold"]["threshold"]:
        res.append({"signal_type": "RSI中立以下", "reason": f"RSI: {row['rsi_14']:.1f}", "priority": 2})

    # ③ 出来高急増
    if row["volume_ratio"] >= cfg["signals"]["volume_surge"]["multiplier"]:
        res.append({"signal_type": "出来高急増", "reason": f"出来高 {row['volume_ratio']:.1f}倍", "priority": 3})

    if not res: return []

    # 1銘柄1シグナルに絞り、計算値を付与
    best_signal = sorted(res, key=lambda x: x["priority"])[0]
    best_signal.update({
        "ticker": ticker, 
        "price": price, 
        **row.to_dict() # スコア計算に必要な全指標（乖離率等）を統合
    })
    return [best_signal]

# -----------------------------------------------------------------------
# メインスキャン
# -----------------------------------------------------------------------
# signal_engine.py の scan_signals() を修正
def scan_signals(daily_data: pd.DataFrame, market_status: str = None) -> list[dict]:
    if daily_data.empty: return []
    cfg = _load_config()

    # 地合いの確定
    market_change, market_status_actual = _get_market_condition()
    if market_status is None:
        market_status = market_status_actual

    # ★ market_breaker チェック（ここを追加）
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
