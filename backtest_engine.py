import os
import requests
import pandas as pd
from google import genai

from database_manager import DBManager
from signal_engine import _check_signals, _load_config
from scoring_system import calculate_score

# -----------------------------------------------------------------------
# バックテストパラメータ
# -----------------------------------------------------------------------
_DEFAULT_BT_PARAMS = {
    "hold_days":        10,
    "stop_loss_pct":     5.0,
    "initial_capital":  1_000_000,
    "position_size":    0.1,
    "max_daily_entries": 3, 
}

def _load_bt_params() -> dict:
    cfg = _load_config()
    bt  = cfg.get("backtest", {})
    return {**_DEFAULT_BT_PARAMS, **bt}

# -----------------------------------------------------------------------
# 単一トレードのシミュレーション
# -----------------------------------------------------------------------
def _execute_trade(sig: dict, bt_params: dict) -> dict:
    df = sig["df_ticker"]
    idx = sig["entry_idx"] 
    hold_days = bt_params["hold_days"]
    stop_loss = bt_params["stop_loss_pct"] / 100
    
    entry_row = df.iloc[idx]
    entry_price = float(entry_row["open"]) if pd.notna(entry_row["open"]) else float(entry_row["price"])
    if entry_price <= 0: return None

    exit_price = None
    exit_date = None
    exit_reason = "期間満了"

    for j in range(idx + 1, min(idx + 1 + hold_days, len(df))):
        prev_row = df.iloc[j-1] 
        curr_row = df.iloc[j]   
        
        if (float(prev_row["price"]) - entry_price) / entry_price <= -stop_loss:
            exit_price = float(curr_row["open"]) if pd.notna(curr_row["open"]) else float(curr_row["price"])
            exit_date = curr_row["date"]
            exit_reason = "ストップロス(翌朝決済)"
            break

    if exit_price is None:
        exit_idx = min(idx + hold_days, len(df) - 1)
        exit_row = df.iloc[exit_idx]
        exit_price = float(exit_row["open"]) if pd.notna(exit_row["open"]) else float(exit_row["price"])
        exit_date = exit_row["date"]

    pnl_pct = (exit_price - entry_price) / entry_price * 100
    pnl_yen = (bt_params["initial_capital"] * bt_params["position_size"]) * (pnl_pct / 100)

    return {
        "ticker": sig["ticker"],
        "score": sig["score"],
        "signal_type": sig["signal_type"],
        "entry_date": str(entry_row["date"]),
        "entry_price": round(entry_price, 2),
        "exit_date": str(exit_date),
        "exit_price": round(exit_price, 2),
        "pnl_pct": round(pnl_pct, 2),
        "pnl_yen": round(pnl_yen, 0),
        "exit_reason": exit_reason
    }

# -----------------------------------------------------------------------
# 全体集計（スコア細分化対応）
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades:
        return {"total_trades": 0, "win_rate": 0.0, "avg_pnl_pct": 0.0, "total_pnl_yen": 0.0, "max_drawdown_pct": 0.0, "profit_factor": 0.0, "score_analysis": {}}

    df = pd.DataFrame(trades)
    
    # スコアカテゴリ分け
    def categorize_score(s):
        if s < 30: return "30点未満"
        if 30 <= s < 40: return "30点台"
        if 40 <= s < 50: return "40点台"
        if 50 <= s < 60: return "50点台"
        if 60 <= s < 70: return "60点台"
        if 70 <= s < 80: return "70点台"
        return "80点以上"

    df['score_range'] = df['score'].apply(categorize_score)
    order = ["80点以上", "70点台", "60点台", "50点台", "40点台", "30点台", "30点未満"]
    
    score_stats = {}
    for label in order:
        group = df[df['score_range'] == label]
        if not group.empty:
            score_stats[label] = {
                'count': len(group),
                'win_rate': round((group['pnl_pct'] > 0).mean() * 100, 1),
                'avg_return': round(group['pnl_pct'].mean(), 2)
            }

    # 全体集計
    wins = df[df["pnl_pct"] > 0]
    losses = df[df["pnl_pct"] <= 0]
    total_count = len(df)
    win_rate = len(wins) / total_count * 100
    avg_pnl_pct = df["pnl_pct"].mean()
    total_pnl = df["pnl_yen"].sum()
    gross_profit = wins["pnl_yen"].sum() if not wins.empty else 0
    gross_loss = abs(losses["pnl_yen"].sum()) if not losses.empty else 1e-9
    profit_factor = gross_profit / gross_loss

    cumulative = df["pnl_yen"].cumsum()
    peak = cumulative.cummax()
    drawdown = cumulative - peak
    max_dd_pct = (drawdown.min() / bt_params["initial_capital"] * 100)

    return {
        "total_trades": total_count,
        "win_rate": round(win_rate, 1),
        "avg_pnl_pct": round(avg_pnl_pct, 2),
        "total_pnl_yen": round(total_pnl, 0),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "profit_factor": round(profit_factor, 2),
        "score_analysis": score_stats
    }

# (中略: _format_report_with_gemini, _format_report_plain, _send_discord はあなたの提示したものでOK)

# -----------------------------------------------------------------------
# 公開インターフェース（修正版）
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始（精鋭選別モード）...")
    cfg = _load_config()
    bt_params = _load_bt_params()
    
    db = DBManager()
    df_all = db.load_analysis_data(days=365 * 3)
    if df_all.empty: return

    all_potential_signals = []
    tickers = df_all["ticker"].unique()
    print(f"🔍 {len(tickers):,} 銘柄からシグナル抽出...")

    for ticker in tickers:
        df_ticker = df_all[df_all["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        min_days = cfg.get("filter", {}).get("min_data_days", 80)
        for i in range(min_days, len(df_ticker) - 1):
            window_df = df_ticker.iloc[: i + 1]
            if _check_signals(ticker, window_df, cfg):
                entry_row = df_ticker.iloc[i + 1]
                score = calculate_score(entry_row, cfg.get('scoring_logic', {}))
                all_potential_signals.append({
                    "date": entry_row["date"], "ticker": ticker, "score": score,
                    "signal_type": "GoldenCross", "df_ticker": df_ticker, "entry_idx": i + 1
                })

    if not all_potential_signals:
        print("シグナルなし。"); return

    sig_df = pd.DataFrame(all_potential_signals)
    # 日付ごとにスコア上位3つを抽出
    selected_signals = sig_df.sort_values(["date", "score"], ascending=[True, False]).groupby("date").head(bt_params["max_daily_entries"])

    final_trades = []
    ticker_free_date = {} 

    for _, sig in selected_signals.sort_values("date").iterrows():
        ticker = sig["ticker"]
        if ticker in ticker_free_date and sig["date"] < ticker_free_date[ticker]:
            continue
        trade = _execute_trade(sig, bt_params)
        if trade:
            final_trades.append(trade)
            ticker_free_date[ticker] = pd.to_datetime(trade["exit_date"])

    _process_results(final_trades, bt_params)

if __name__ == "__main__":
    run_backtest_and_report()
