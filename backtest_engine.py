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
    "max_daily_entries": 3, # 1日あたりの最大エントリー数
}

def _load_bt_params() -> dict:
    cfg = _load_config()
    bt  = cfg.get("backtest", {})
    return {**_DEFAULT_BT_PARAMS, **bt}

# -----------------------------------------------------------------------
# 単一トレードのシミュレーション（前場成り行き・翌日判定版）
# -----------------------------------------------------------------------
def _execute_trade(sig: dict, bt_params: dict) -> dict:
    """
    確定したシグナル1件に対し、エントリーからエグジットまでを計算する
    """
    df = sig["df_ticker"]
    idx = sig["entry_idx"] # シグナル翌日のインデックス
    hold_days = bt_params["hold_days"]
    stop_loss = bt_params["stop_loss_pct"] / 100
    
    # --- エントリー: 当日の始値 ---
    entry_row = df.iloc[idx]
    entry_price = float(entry_row["open"]) if pd.notna(entry_row["open"]) else float(entry_row["price"])
    if entry_price <= 0: return None

    exit_price = None
    exit_date = None
    exit_reason = "期間満了"

    # --- 保有期間中の監視 ---
    # 翌営業日(idx+1)から期間満了まで
    for j in range(idx + 1, min(idx + 1 + hold_days, len(df))):
        prev_row = df.iloc[j-1] # 前日の終値
        curr_row = df.iloc[j]   # 当日の朝
        
        # 前日終値で損切り判定
        if (float(prev_row["price"]) - entry_price) / entry_price <= -stop_loss:
            exit_price = float(curr_row["open"]) if pd.notna(curr_row["open"]) else float(curr_row["price"])
            exit_date = curr_row["date"]
            exit_reason = "ストップロス(翌朝決済)"
            break

    # 期間満了決済
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
# メイン実行ロジック
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始（1日最大3銘柄 / スコア優先）...")
    cfg = _load_config()
    bt_params = _load_bt_params()
    
    db = DBManager()
    df_all = db.load_analysis_data(days=365 * 3)
    if df_all.empty: return

    # 1. 全銘柄からシグナルを抽出（全日程スキャン）
    all_potential_signals = []
    tickers = df_all["ticker"].unique()
    print(f"🔍 {len(tickers):,} 銘柄からシグナルを抽出中...")

    for ticker in tickers:
        df_ticker = df_all[df_all["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        min_days = cfg.get("filter", {}).get("min_data_days", 80)
        
        for i in range(min_days, len(df_ticker) - 1):
            # i = 当日(シグナル発生日), i+1 = 翌日(エントリー日)
            window_df = df_ticker.iloc[: i + 1]
            hits = _check_signals(ticker, window_df, cfg)
            
            if hits:
                entry_row = df_ticker.iloc[i + 1]
                score = calculate_score(entry_row, cfg.get('scoring_logic', {}))
                all_potential_signals.append({
                    "date": entry_row["date"], # 執行日
                    "ticker": ticker,
                    "score": score,
                    "signal_type": hits[0]["signal_type"],
                    "df_ticker": df_ticker,
                    "entry_idx": i + 1
                })

    if not all_potential_signals:
        print("シグナルなし。")
        return

    # 2. 日付ごとにフィルタリング（スコア上位3位まで）
    sig_df = pd.DataFrame(all_potential_signals)
    print(f"⚖️ スコアによる選別実行中 (候補数: {len(sig_df)})...")
    
    # 日付ごとにスコア降順でソートし、上位3つを抽出
    selected_signals = sig_df.sort_values(["date", "score"], ascending=[True, False]).groupby("date").head(bt_params["max_daily_entries"])

    # 3. 選別されたシグナルを時系列順に執行
    final_trades = []
    ticker_free_date = {} # 銘柄ごとの拘束終了日

    for _, sig in selected_signals.sort_values("date").iterrows():
        ticker = sig["ticker"]
        exec_date = sig["date"]

        # 同一銘柄がまだ保有中（または同日重複）ならスキップ
        if ticker in ticker_free_date and exec_date < ticker_free_date[ticker]:
            continue

        trade = _execute_trade(sig, bt_params)
        if trade:
            final_trades.append(trade)
            # 次にエントリー可能になるのは、決済日の「翌日」以降
            ticker_free_date[ticker] = pd.to_datetime(trade["exit_date"])

    # 4. 集計と報告
    _process_results(final_trades, bt_params)

def _process_results(trades, bt_params):
    if not trades:
        print("有効なトレードはありませんでした。")
        return

    summary = _calc_summary(trades, bt_params)
    
    # ターミナル表示
    print("\n" + "="*30)
    print(f"✅ バックテスト完了: {len(trades)} トレード (精鋭選別後)")
    print(f"勝率: {summary['win_rate']}% | 平均損益: {summary['avg_pnl_pct']}%")
    print(f"PF: {summary['profit_factor']} | 最大DD: {summary['max_drawdown_pct']}%")
    print("="*30)

    # Gemini & Discord処理 (既存の関数を呼び出し)
    top_trades = sorted(trades, key=lambda x: x["pnl_pct"], reverse=True)
    report = _format_report_with_gemini(summary, top_trades)
    _send_discord("📈 **【精鋭選別バックテストレポート】**\n" + report)

# (以下、_calc_summary, _format_report_with_gemini, _send_discord 等は既存のまま)
