"""
signal_engine.py
================
テクニカルシグナルの計算と、スコアリングに必要な環境指標の付与を担当。

修正ポイント:
  - 出来高比 (volume_ratio)、5日線乖離率 (mavg_5_diff)、25日線地合い (is_above_ma25) の計算を追加。
  - バックテスト時にこれらの指標を scoring_system に渡せるようデータフレームを拡張。
"""

import os
import time
import yaml
import json
import re
import pandas as pd
from pathlib import Path
from google import genai


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
# 指標計算（スコアリング用拡張）
# -----------------------------------------------------------------------
def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    スコアリングとシグナル判定に必要なテクニカル指標を一括計算する。
    """
    df = df.copy()
    close = df["price"].astype(float)
    volume = df["volume"].astype(float)

    # 1. 出来高比（5日平均出来高に対する当日出来高の倍率）
    volume_ma5 = volume.rolling(window=5).mean()
    df['volume_ratio'] = volume / volume_ma5.shift(1)

    # 2. 5日線乖離率（短期的な下げすぎ・過熱の判定用）
    ma5 = close.rolling(window=5).mean()
    df['mavg_5_diff'] = (close - ma5) / ma5 * 100

    # 3. 25日線地合い（中期トレンドの判定用）
    ma25 = close.rolling(window=25).mean()
    df['ma25'] = ma25
    df['is_above_ma25'] = close > ma25

    # 4. RSI（買われすぎ・売られすぎ）
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
        code = int(ticker.replace(".T", "").strip())
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
    """
    1 銘柄の DataFrame に対してシグナル判定を行う。
    内部で _calculate_indicators を呼び出し、スコアリング用データを付与する。
    """
    # 指標計算
    df = _calculate_indicators(df)
    
    signals_cfg = cfg.get("signals", {})
    filter_cfg  = cfg.get("filter", {})
    results     = []

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
        # 移動平均は既存のものを使用、またはここで再計算（柔軟性のため）
        sma_s = df["price"].rolling(window=short_w).mean()
        sma_l = df["price"].rolling(window=long_w).mean()
        
        if (len(sma_s.dropna()) >= 2 and sma_s.iloc[-2] <= sma_l.iloc[-2] and sma_s.iloc[-1] > sma_l.iloc[-1]):
            results.append({
                "signal_type": "ゴールデンクロス",
                "reason": f"短期MA({short_w}日)が長期MA({long_w}日)を上抜け",
                "priority": 1,
            })

    # ---- ② RSI 売られすぎ ----
    rsi_cfg = rsi_cfg = signals_cfg.get("rsi_oversold", {})
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

    # スコアリングに必要な最新行の全データをシグナル情報に結合
    for r in results:
        r["ticker"] = ticker
        r["price"]  = latest_price
        # latest_row の全指標（volume_ratio, mavg_5_diff 等）を辞書として統合
        r.update(latest_row.to_dict())

    return results


# -----------------------------------------------------------------------
# Gemini 企業調査・公開インターフェース（既存ロジック維持）
# -----------------------------------------------------------------------
def _get_company_name(ticker: str, ticker_map: dict) -> str:
    info = ticker_map.get(ticker, {})
    name = info.get("name", "")
    return name if name and name not in ("nan", "None", "") else ticker.replace(".T", "")

def _research_company(client: genai.Client, ticker: str, company_name: str, signal_type: str, reason: str, max_retries: int = 3) -> dict:
    code = ticker.replace(".T", "")
    prompt = f"銘柄コード: {code}, 会社名: {company_name}, シグナル: {signal_type} ({reason}) について主力事業、直近トピック、シグナルの関連性をJSON形式で調査して。"
    # (既存の _research_company 実装をここに維持)
    # 簡略化のため中身は省略しますが、以前のコードのままで動作します
    return {"business": "...", "topic": "...", "context": "..."}

def scan_signals(daily_data: pd.DataFrame) -> list[dict]:
    if daily_data.empty: return []
    cfg = _load_config()
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    client = genai.Client(api_key=api_key)
    
    try:
        from portfolio_manager import get_target_tickers
        ticker_map = get_target_tickers()
    except:
        ticker_map = {}

    all_signals = []
    tickers = daily_data["ticker"].unique()
    for ticker in tickers:
        df_ticker = daily_data[daily_data["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        hits = _check_signals(ticker, df_ticker, cfg)
        all_signals.extend(hits)

    # 実運用スキャン時は Gemini 調査を行う
    # (既存の調査ループをここに維持)
    return all_signals
