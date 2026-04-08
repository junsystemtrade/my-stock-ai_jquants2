"""
signal_engine.py
================
テクニカルシグナルの計算と、スコアリングに必要な環境指標の付与を担当。
日経平均先物(NIY=F)の地合い判定を全シグナルに統合。
"""

import os
import time
import yaml
import json
import re
import pandas as pd
from pathlib import Path
from google import genai
from sqlalchemy import text

import database_manager

# -----------------------------------------------------------------------
# シグナル設定の読み込み
# -----------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent / "signals_config.yml"

def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {
        "signals": {
            "golden_cross": {"enabled": True, "short_window": 25, "long_window": 75},
            "rsi_oversold":  {"enabled": True, "window": 14, "threshold": 30},
            "volume_surge":  {"enabled": True, "window": 20, "multiplier": 2.0},
        },
        "filter": {
            "min_price":            500,
            "max_price":          50000,
            "min_data_days":         80,
            "exclude_code_range": [[1000, 1999]],
            "max_signals_per_ticker": 1,
        },
    }

# -----------------------------------------------------------------------
# 地合い判定（NIY=F活用）
# -----------------------------------------------------------------------
def _get_market_condition() -> tuple[float, str]:
    """
    データベースから日経平均先物(NIY=F)の直近2日分を取得し、地合いを計算。
    """
    db = database_manager.DBManager()
    query = text("""
        SELECT price, date FROM daily_prices 
        WHERE ticker = 'NIY=F' 
        ORDER BY date DESC LIMIT 2
    """)
    
    try:
        with db.engine.connect() as conn:
            df_niy = pd.read_sql(query, conn)
        
        if len(df_niy) < 2:
            return 0.0, "不明"

        latest_price = float(df_niy['price'].iloc[0])
        prev_price = float(df_niy['price'].iloc[1])
        market_change_pct = (latest_price - prev_price) / prev_price * 100
        
        if market_change_pct <= -2.0:
            status = "暴落警戒"
        elif market_change_pct <= -0.5:
            status = "軟調"
        elif market_change_pct >= 0.5:
            status = "好調"
        else:
            status = "平穏"
            
        return market_change_pct, status
    except Exception as e:
        print(f" [Warning] Market condition check failed: {e}")
        return 0.0, "エラー"

# -----------------------------------------------------------------------
# 指標計算（スコアリング用拡張）
# -----------------------------------------------------------------------
def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close = df["price"].astype(float)
    volume = df["volume"].astype(float)

    # 1. 出来高比
    volume_ma5 = volume.rolling(window=5).mean()
    df['volume_ratio'] = volume / volume_ma5.shift(1)

    # 2. 5日線乖離率
    ma5 = close.rolling(window=5).mean()
    df['mavg_5_diff'] = (close - ma5) / ma5 * 100

    # 3. 25日線地合い & トレンドの向き
    ma25 = close.rolling(window=25).mean()
    df['ma25'] = ma25
    df['is_above_ma25'] = close > ma25
    df['ma25_upward'] = ma25 > ma25.shift(1)

    # 4. RSI
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(window=14).mean()
    loss = (-delta.clip(upper=0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, float("nan"))
    df['rsi_14'] = 100 - (100 / (1 + rs))

    return df

# -----------------------------------------------------------------------
# ETF・REIT 除外チェック
# -----------------------------------------------------------------------
def _is_excluded(ticker: str, cfg: dict) -> bool:
    filter_cfg = cfg.get("filter", {})
    exclude_ranges = filter_cfg.get("exclude_code_range", [])

    try:
        code_str = ticker.replace(".T", "").strip()
        if not code_str.isdigit(): return False
        code = int(code_str)
    except ValueError:
        return False

    for r in exclude_ranges:
        if len(r) == 2 and r[0] <= code <= r[1]:
            return True
    return False

# -----------------------------------------------------------------------
# シグナル判定（1 銘柄ぶん）
# -----------------------------------------------------------------------
def _check_signals(ticker: str, df: pd.DataFrame, cfg: dict) -> list[dict]:
    # 指標計算
    df = _calculate_indicators(df)
    
    signals_cfg = cfg.get("signals", {})
    filter_cfg  = cfg.get("filter", {})
    results      = []

    if _is_excluded(ticker, cfg):
        return []

    min_days = filter_cfg.get("min_data_days", 80)
    if len(df) < min_days:
        return []

    latest_row = df.iloc[-1]
    latest_price = float(latest_row["price"])

    # 価格フィルター
    min_p = filter_cfg.get("min_price", 500)
    max_p = filter_cfg.get("max_price", 50000)
    if not (min_p <= latest_price <= max_p):
        return []

    # ---- ① ゴールデンクロス ----
    gc_cfg = signals_cfg.get("golden_cross", {})
    if gc_cfg.get("enabled", True):
        short_w = gc_cfg.get("short_window", 25)
        long_w  = gc_cfg.get("long_window", 75)
        sma_s = df["price"].rolling(window=short_w).mean()
        sma_l = df["price"].rolling(window=long_w).mean()
        
        if (len(sma_s.dropna()) >= 2 and sma_s.iloc[-2] <= sma_l.iloc[-2] and sma_s.iloc[-1] > sma_l.iloc[-1]):
            results.append({
                "signal_type": "ゴールデンクロス",
                "reason": f"短期MA({short_w}日)が長期MA({long_w}日)を上抜け",
                "priority": 1,
            })

    # ---- ② RSI 売られすぎ ----
    rsi_cfg = signals_cfg.get("rsi_oversold", {})
    if rsi_cfg.get("enabled", True):
        threshold = rsi_cfg.get("threshold", 30)
        rsi_val = latest_row["rsi_14"]
        if not pd.isna(rsi_val) and rsi_val < threshold:
            results.append({
                "signal_type": "RSI売られすぎ",
                "reason": f"RSI: {rsi_val:.1f} (閾値 {threshold} 以下)",
                "priority": 2,
            })

    # ---- ③ 出来高急増 ----
    vol_cfg = signals_cfg.get("volume_surge", {})
    if vol_cfg.get("enabled", True):
        mult = vol_cfg.get("multiplier", 2.0)
        v_ratio = latest_row["volume_ratio"]
        if not pd.isna(v_ratio) and v_ratio >= mult:
            results.append({
                "signal_type": "出来高急増",
                "reason": f"出来高が5日平均の {v_ratio:.1f}倍に急増",
                "priority": 3,
            })

    if not results:
        return []

    # 優先順位で絞り込み
    max_signals = filter_cfg.get("max_signals_per_ticker", 1)
    results = sorted(results, key=lambda x: x["priority"])[:max_signals]

    # スコアリング用データを結合
    for r in results:
        r["ticker"] = ticker
        r["price"]  = latest_price
        r.update(latest_row.to_dict())

    return results

# -----------------------------------------------------------------------
# メインスキャン
# -----------------------------------------------------------------------
def scan_signals(daily_data: pd.DataFrame) -> list[dict]:
    if daily_data.empty: return []
    cfg = _load_config()
    
    # 地合いデータの取得
    m_change, m_status = _get_market_condition()
    print(f"📊 市場地合い: {m_status} (NIY=F 前日比: {m_change:.2f}%)")

    all_signals = []
    tickers = daily_data["ticker"].unique()
    
    for ticker in tickers:
        # 指標銘柄自体はスキャン対象外
        if ticker == "NIY=F": continue
        
        df_ticker = daily_data[daily_data["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        hits = _check_signals(ticker, df_ticker, cfg)
        
        # 全シグナルに地合い情報を付与
        for h in hits:
            h["market_change_pct"] = m_change
            h["market_status"] = m_status
        
        all_signals.extend(hits)

    return all_signals
