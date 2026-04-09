"""
backtest.py
===========
市場環境（地合い）とスコアリングを用いた精鋭選別バックテスト。
2点刻みの詳細分析機能を搭載。
"""

import os
import requests
import pandas as pd
from google import genai
from datetime import datetime, timedelta

import portfolio_manager
from database_manager import DBManager
from signal_engine import _check_signals, _load_config
from scoring_system import calculate_score

# -----------------------------------------------------------------------
# バックテストパラメータ
# -----------------------------------------------------------------------
_DEFAULT_BT_PARAMS = {
    "hold_days":          10,
    "stop_loss_pct":      5.0,
    "initial_capital":    1_000_000,
    "position_size":      0.1,
    "max_daily_entries": 3, 
    "market_crash_limit": -2.0  # 日経先物が前日比-2%以下ならエントリー見送り
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
    entry_price = float(entry_row["open"]) if pd.notna(entry_row.get("open")) and entry_row["open"] > 0 else float(entry_row["price"])
    if entry_price <= 0: return None

    exit_price, exit_date, exit_reason = None, None, "期間満了"

    for j in range(idx + 1, min(idx + 1 + hold_days, len(df))):
        curr_row = df.iloc[j]
        low_price = float(curr_row["low"]) if pd.notna(curr_row.get("low")) else float(curr_row["price"])
        
        if (low_price - entry_price) / entry_price <= -stop_loss:
            exit_price = entry_price * (1 - stop_loss)
            exit_date = curr_row["date"]
            exit_reason = "ストップロス"
            break

    if exit_price is None:
        exit_idx = min(idx + hold_days, len(df) - 1)
        exit_row = df.iloc[exit_idx]
        exit_price = float(exit_row["open"]) if pd.notna(exit_row.get("open")) and exit_row["open"] > 0 else float(exit_row["price"])
        exit_date = exit_row["date"]

    pnl_pct = (exit_price - entry_price) / entry_price * 100
    pnl_yen = (bt_params["initial_capital"] * bt_params["position_size"]) * (pnl_pct / 100)

    return {
        "ticker": sig["ticker"],
        "score": sig["score"],
        "signal_type": sig["signal_type"],
        "entry_date": entry_row["date"],
        "entry_price": round(entry_price, 2),
        "exit_date": exit_date,
        "exit_price": round(exit_price, 2),
        "pnl_pct": round(pnl_pct, 2),
        "pnl_yen": round(pnl_yen, 0),
        "exit_reason": exit_reason
    }

# -----------------------------------------------------------------------
# 集計・レポート
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades: return {"total_trades": 0}

    df = pd.DataFrame(trades)
    
    # 【核心】2点刻みでスコアをグルーピング
    df['score_bin'] = (df['score'] // 2) * 2

    score_stats = {}
    grouped = df.groupby('score_bin')
    for bin_val, group in grouped:
        wins = group[group['pnl_pct'] > 0]
        losses = group[group['pnl_pct'] <= 0]
        
        gross_profit = wins['pnl_yen'].sum()
        gross_loss = abs(losses['pnl_yen'].sum()) or 1e-9
        
        score_stats[bin_val] = {
            'count': len(group),
            'win_rate': round(len(wins) / len(group) * 100, 1),
            'avg_return': round(group['pnl_pct'].mean(), 2),
            'pf': round(gross_profit / gross_loss, 2)
        }

    wins = df[df["pnl_pct"] > 0]
    losses = df[df["pnl_pct"] <= 0]
    total_pnl = df["pnl_yen"].sum()
    gross_profit = wins["pnl_yen"].sum()
    gross_loss = abs(losses["pnl_yen"].sum()) or 1e-9

    cumulative = df["pnl_yen"].cumsum()
    max_dd = (cumulative - cumulative.cummax()).min()

    return {
        "total_trades": len(df),
        "win_rate": round(len(wins) / len(df) * 100, 1),
        "avg_pnl_pct": round(df["pnl_pct"].mean(), 2),
        "total_pnl_yen": round(total_pnl, 0),
        "max_drawdown_pct": round(max_dd / bt_params["initial_capital"] * 100, 2),
        "profit_factor": round(gross_profit / gross_loss, 2),
        "score_analysis": score_stats
    }

def _format_report_plain(summary: dict) -> str:
    # 2点刻みで内訳を表示するフォーマット
    score_brief = "\n【スコア別詳細分析（2点刻み）】\n"
    score_brief += "------------------------------------------------------------\n"
    sorted_scores = sorted(summary.get('score_analysis', {}).items(), key=lambda x: x[0], reverse=True)
    
    for bin_val, v in sorted_scores:
        label = f"{bin_val:2.0f}-{bin_val+1.9:4.1f}点"
        score_brief += f"{label}: {v['count']:>3}回 | 勝率{v['win_rate']:>5}% | 平均{v['avg_return']:>+6.2f}% | PF:{v['pf']:>4.2f}\n"
    
    return (
        f"📊 **バックテスト結果(精鋭モード)**\n"
        f"総数: {summary['total_trades']}回 / 勝率: {summary['win_rate']}%\n"
        f"平均損益: {summary['avg_pnl_pct']:+.2f}% / 合計: {summary['total_pnl_yen']:+,.0f}円\n"
        f"PF: {summary['profit_factor']} / 最大DD: {summary['max_drawdown_pct']}%\n"
        f"{score_brief}"
    )

def _format_report_with_gemini(summary: dict, top_trades: list[dict]) -> str:
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key: return _format_report_plain(summary)
    
    client = genai.Client(api_key=api_key)
    prompt = f"日本株バックテスト結果です。2点刻みのスコア分析から、どのスコア帯が最も効率的か、改善案を要約して。\n\n結果:\n{summary}\n\n上位:\n{top_trades[:3]}"
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return response.text.strip()
    except:
        return _format_report_plain(summary)

def _send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if url: requests.post(url, json={"content": content[:1990]})

# -----------------------------------------------------------------------
# メイン実行
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始...")
    cfg, bt_params = _load_config(), _load_bt_params()
    db = DBManager()
    df_all = db.load_analysis_data(days=365 * 3)
    if df_all.empty: return

    niy_df = df_all[df_all["ticker"] == "NIY=F"].sort_values("date").copy()
    niy_df["m_change"] = niy_df["price"].pct_change() * 100
    crash_dates = set(niy_df[niy_df["m_change"] <= bt_params["market_crash_limit"]]["date"])
    
    all_signals = []
    tickers = [t for t in df_all["ticker"].unique() if t != "NIY=F"]
    min_days = cfg.get("filter", {}).get("min_data_days", 80)

    for ticker in tickers:
        df_ticker = df_all[df_all["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        if len(df_ticker) < min_days: continue
        
        for i in range(min_days, len(df_ticker) - 1):
    entry_date = df_ticker.iloc[i + 1]["date"]
    if entry_date in crash_dates: continue

    # _check_signals は内部で _calculate_indicators を呼び、
    # 指標を含んだ辞書（hits）を返すように設計されているため、これを利用する
    hits = _check_signals(ticker, df_ticker.iloc[:i+1], cfg)
    if hits:
        # hits[0] には row.to_dict() によって全指標（mavg_25_diff等）が含まれている
        score = calculate_score(pd.Series(hits[0]), cfg.get('scoring_logic', {}))
        all_signals.append({
            "date": pd.to_datetime(entry_date), 
            "ticker": ticker, 
            "score": score,
            "signal_type": hits[0]["signal_type"], 
            "df_ticker": df_ticker, 
            "entry_idx": i + 1
        })

    if not all_signals:
        print("シグナルが検出されませんでした。"); return

    sig_df = pd.DataFrame(all_signals)
    selected = sig_df.sort_values(["date", "score"], ascending=[True, False]).groupby("date").head(bt_params["max_daily_entries"])

    final_trades, free_dates = [], {} 

    for _, sig in selected.iterrows():
        ticker = sig["ticker"]
        if ticker in free_dates and sig["date"] < pd.to_datetime(free_dates[ticker]): continue

        trade = _execute_trade(sig, bt_params)
        if trade:
            final_trades.append(trade)
            free_dates[ticker] = trade["exit_date"]

    if final_trades:
        summary = _calc_summary(final_trades, bt_params)
        report = _format_report_with_gemini(summary, sorted(final_trades, key=lambda x: x["pnl_pct"], reverse=True))
        _send_discord(f"📈 **精鋭バックテストレポート**\n{report}")
        print(_format_report_plain(summary))

if __name__ == "__main__":
    run_backtest_and_report()
