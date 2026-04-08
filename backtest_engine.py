import os
import requests
import pandas as pd
from google import genai
from datetime import datetime

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
    # エントリーは翌朝の始値を想定（始値がなければprice）
    entry_price = float(entry_row["open"]) if pd.notna(entry_row["open"]) and entry_row["open"] > 0 else float(entry_row["price"])
    if entry_price <= 0: return None

    exit_price = None
    exit_date = None
    exit_reason = "期間満了"

    # ストップロス判定（期間中の安値をチェック）
    for j in range(idx + 1, min(idx + 1 + hold_days, len(df))):
        curr_row = df.iloc[j]
        low_price = float(curr_row["low"]) if pd.notna(curr_row["low"]) else float(curr_row["price"])
        
        if (low_price - entry_price) / entry_price <= -stop_loss:
            # 損切りは翌日の始値（または損切りライン）で実行
            exit_price = entry_price * (1 - stop_loss)
            exit_date = curr_row["date"]
            exit_reason = "ストップロス"
            break

    # 期間満了時の決済
    if exit_price is None:
        exit_idx = min(idx + hold_days, len(df) - 1)
        exit_row = df.iloc[exit_idx]
        exit_price = float(exit_row["open"]) if pd.notna(exit_row["open"]) and exit_row["open"] > 0 else float(exit_row["price"])
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
# 集計・レポート補助関数
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades:
        return {"total_trades": 0, "win_rate": 0, "avg_pnl_pct": 0, "total_pnl_yen": 0, "max_drawdown_pct": 0, "profit_factor": 0, "score_analysis": {}}

    df = pd.DataFrame(trades)
    
    def categorize_score(s):
        if s < 30: return "30点未満"
        if 30 <= s < 40: return "30点台"
        if 40 <= s < 50: return "40点台"
        if 50 <= s < 60: return "50点台"
        if 60 <= s < 70: return "60点台"
        if 70 <= s < 80: return "70点台"
        return "80点以上"

    df['score_range'] = df['score'].apply(categorize_score)
    score_stats = {}
    for label in ["80点以上", "70点台", "60点台", "50点台", "40点台", "30点台", "30点未満"]:
        group = df[df['score_range'] == label]
        if not group.empty:
            score_stats[label] = {
                'count': len(group),
                'win_rate': round((group['pnl_pct'] > 0).mean() * 100, 1),
                'avg_return': round(group['pnl_pct'].mean(), 2)
            }

    wins = df[df["pnl_pct"] > 0]
    losses = df[df["pnl_pct"] <= 0]
    total_pnl = df["pnl_yen"].sum()
    gross_profit = wins["pnl_yen"].sum() if not wins.empty else 0
    gross_loss = abs(losses["pnl_yen"].sum()) if not losses.empty else 1e-9

    cumulative = df["pnl_yen"].cumsum()
    peak = cumulative.cummax()
    max_dd = (cumulative - peak).min()

    return {
        "total_trades": len(df),
        "win_rate": round(len(wins) / len(df) * 100, 1) if len(df) > 0 else 0,
        "avg_pnl_pct": round(df["pnl_pct"].mean(), 2),
        "total_pnl_yen": round(total_pnl, 0),
        "max_drawdown_pct": round(max_dd / bt_params["initial_capital"] * 100, 2),
        "profit_factor": round(gross_profit / gross_loss, 2),
        "score_analysis": score_stats
    }

def _format_report_plain(summary: dict) -> str:
    score_brief = "\n【スコア別分析】\n"
    for k, v in summary['score_analysis'].items():
        score_brief += f"{k}: {v['count']}回 | 勝率{v['win_rate']}% | 平均{v['avg_return']:+.2f}%\n"
    
    return (
        f"📊 **バックテスト結果(厳選モード)**\n"
        f"総数: {summary['total_trades']}回 / 勝率: {summary['win_rate']}%\n"
        f"平均損益: {summary['avg_pnl_pct']:+.2f}% / 合計: {summary['total_pnl_yen']:+,.0f}円\n"
        f"PF: {summary['profit_factor']} / 最大DD: {summary['max_drawdown_pct']}%\n"
        f"{score_brief}"
    )

def _format_report_with_gemini(summary: dict, top_trades: list[dict]) -> str:
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key: return _format_report_plain(summary)
    
    client = genai.Client(api_key=api_key)
    prompt = f"以下は独自スコアリングを用いた日本株バックテスト結果です。市場環境（地合い）を考慮した取引の結果について分析し、今後の戦略へのアドバイスをDiscord用に要約して。\n\n結果:\n{summary}\n\n上位成功例:\n{top_trades[:3]}"
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return response.text.strip()
    except:
        return _format_report_plain(summary)

def _send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if url: requests.post(url, json={"content": content[:1990]})

def _process_results(trades, bt_params):
    if not trades:
        print("有効なトレードなし"); return
    summary = _calc_summary(trades, bt_params)
    plain_text = _format_report_plain(summary)
    print(plain_text)
    report = _format_report_with_gemini(summary, sorted(trades, key=lambda x: x["pnl_pct"], reverse=True))
    _send_discord(f"📈 **精鋭バックテストレポート**\n{report}")

# -----------------------------------------------------------------------
# メイン
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始（地合いフィルター & 精鋭選別モード）...")
    cfg = _load_config()
    bt_params = _load_bt_params()
    db = DBManager()
    df_all = db.load_analysis_data(days=365 * 3)
    if df_all.empty: return

    # --- 市場地合い(NIY=F)データの事前準備 ---
    niy_df = df_all[df_all["ticker"] == "NIY=F"].sort_values("date").copy()
    niy_df["market_change"] = niy_df["price"].pct_change() * 100
    market_crash_dates = niy_df[niy_df["market_change"] <= bt_params["market_crash_limit"]]["date"].tolist()
    
    all_potential_signals = []
    tickers = [t for t in df_all["ticker"].unique() if t != "NIY=F"]
    print(f"🔍 {len(tickers):,} 銘柄からシグナル抽出...")

    for ticker in tickers:
        df_ticker = df_all[df_all["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        min_days = cfg.get("filter", {}).get("min_data_days", 80)
        
        for i in range(min_days, len(df_ticker) - 1):
            entry_row = df_ticker.iloc[i + 1]
            entry_date = entry_row["date"]
            
            # 【地合いフィルター】前日に日経先物が暴落していたら、翌日のエントリーは見送り
            if entry_date in market_crash_dates:
                continue

            window_df = df_ticker.iloc[: i + 1]
            hits = _check_signals(ticker, window_df, cfg)
            
            if hits:
                # スコア計算
                score = calculate_score(entry_row, cfg.get('scoring_logic', {}))
                all_potential_signals.append({
                    "date": entry_date, "ticker": ticker, "score": score,
                    "signal_type": hits[0]["signal_type"], "df_ticker": df_ticker, "entry_idx": i + 1
                })

    if not all_potential_signals:
        print("シグナルなし"); return

    sig_df = pd.DataFrame(all_potential_signals)
    sig_df["date"] = pd.to_datetime(sig_df["date"])
    
    # 日ごとにスコア上位のみを選択
    selected_signals = sig_df.sort_values(["date", "score"], ascending=[True, False]).groupby("date").head(bt_params["max_daily_entries"])

    final_trades = []
    ticker_free_date = {} 

    for _, sig in selected_signals.sort_values("date").iterrows():
        ticker = sig["ticker"]
        exec_ts = sig["date"]

        # 同一銘柄の保有期間重複を避ける
        if ticker in ticker_free_date and exec_ts < pd.to_datetime(ticker_free_date[ticker]):
            continue

        trade = _execute_trade(sig, bt_params)
        if trade:
            final_trades.append(trade)
            ticker_free_date[ticker] = trade["exit_date"]

    _process_results(final_trades, bt_params)

if __name__ == "__main__":
    run_backtest_and_report()
