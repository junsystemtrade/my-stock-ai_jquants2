# signal_engine.py: YAMLとScoringを統合したメインエンジン
import yaml
import pandas as pd
from pathlib import Path
import scoring_system

_CONFIG_PATH = Path(__file__).parent / "signals_config.yml"

def _load_config():
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {}

def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close, vol = df["price"].astype(float), df["volume"].astype(float)
    df["volume_ratio"] = vol / vol.rolling(5).mean().shift(1)
    df["volume_avg_20"] = vol.rolling(20).mean()
    ma5, ma25 = close.rolling(5).mean(), close.rolling(25).mean()
    df["sma_5"], df["sma_25"] = ma5, ma25
    df["mavg_25_diff"] = (close - ma25) / ma25 * 100
    df["ma25_upward"] = ma25.diff() > 0
    df["turnover_avg_20"] = (close * vol).rolling(20).mean()
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df["rsi_14"] = 100 - (100 / (1 + (gain / loss.replace(0, float("nan")))))
    return df

def scan_signals(daily_data: pd.DataFrame, market_status: str = "不明") -> list[dict]:
    cfg = _load_config()
    min_score = cfg.get("backtest", {}).get("min_score", 70.0) # 精鋭足切り
    all_hits = []
    
    for ticker, df_ticker in daily_data.groupby("ticker"):
        df_calc = _calculate_indicators(df_ticker.sort_values("date"))
        row = df_calc.iloc[-1]
        
        # 基本フィルタ (YAML同期)
        if row["turnover_avg_20"] < cfg["filter"]["min_daily_turnover_avg_20"]: continue
        
        # GC判定
        if len(df_calc) >= 2 and df_calc["sma_5"].iloc[-2] <= df_calc["sma_25"].iloc[-2] and df_calc["sma_5"].iloc[-1] > df_calc["sma_25"].iloc[-1]:
            # スコア計算
            score = scoring_system.calculate_score(row, cfg.get("scoring_logic"))
            if score >= min_score:
                h = row.to_dict()
                h.update({"ticker": ticker, "score": score, "market_status": market_status})
                all_hits.append(h)
    return all_hits

def check_exit_signals(daily_data: pd.DataFrame) -> list[dict]:
    cfg = _load_config()
    rsi_limit = cfg["exit_rules"]["immediate"]["rsi_overbought"] # 80に同期
    # ...（保有銘柄ループと決済判定ロジック）...
    return []
