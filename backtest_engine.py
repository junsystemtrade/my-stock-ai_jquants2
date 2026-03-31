"""
backtest_engine.py
==================
過去2年分のデータを用いてバックテストを実行し、
結果の統計情報を Gemini で分析して Discord へ報告する。
"""

import os
import requests
import pandas as pd
import google.generativeai as genai
from database_manager import DBManager
from signal_engine import _check_signals, _load_config

# -----------------------------------------------------------------------
# バックテストパラメータ
# -----------------------------------------------------------------------
_DEFAULT_BT_PARAMS = {
    "hold_days":      10,    # 最大保有日数
    "stop_loss_pct":   5.0,  # ストップロス（%）
    "initial_capital": 1_000_000,
    "position_size":   0.1,  # 1件あたり資金の10%を投入
}

def _load_bt_params() -> dict:
    cfg = _load_config()
    bt  = cfg.get("backtest", {})
    return {**_DEFAULT_BT_PARAMS, **bt}

# -----------------------------------------------------------------------
# 1 銘柄のバックテスト
# -----------------------------------------------------------------------
def _backtest_ticker(ticker: str, df: pd.DataFrame, cfg: dict, bt_params: dict) -> list[dict]:
    hold_days   = bt_params["hold_days"]
    stop_loss   = bt_params["stop_loss_pct"] / 100
    pos_size    = bt_params["position_size"]
    capital     = bt_params["initial_capital"]

    trades      = []
    in_position = False
    min_days    = cfg.get("filter", {}).get("min_data_days", 80)

    for i in range(min_days, len(df)):
        window_df = df.iloc[: i + 1].copy()
        if not in_position:
            hits = _check_signals(ticker, window_df, cfg)
            if not hits: continue
            if i + 1 >= len(df): continue

            entry_row   = df.iloc[i + 1]
            entry_date  = entry_row["date"]
            entry_price = float(entry_row["open"]) if pd.notna(entry_row["open"]) else float(entry_row["price"])
            signal_type = hits[0]["signal_type"]
            in_position = True

            exit_price  = None
            exit_date   = None
            exit_reason = "期間満了"

            for j in range(i + 2, min(i + 2 + hold_days, len(df))):
                row      = df.iloc[j]
                close    = float(row["price"])
                pnl_rate = (close - entry_price) / entry_price
                if pnl_rate <= -stop_loss:
                    exit_price  = close
                    exit_date   = row["date"]
                    exit_reason = f"ストップロス({stop_loss*100:.1f}%)"
                    break

            if exit_price is None:
                exit_idx    = min(i + 1 + hold_days, len(df) - 1)
                exit_price  = float(df.iloc[exit_idx]["price"])
                exit_date   = df.iloc[exit_idx]["date"]

            pnl_pct = (exit_price - entry_price) / entry_price * 100
            trades.append({
                "ticker": ticker, "signal_type": signal_type,
                "entry_date": str(entry_date), "entry_price": round(entry_price, 2),
                "exit_date": str(exit_date), "exit_price": round(exit_price, 2),
                "pnl_pct": round(pnl_pct, 2), "pnl_yen": round((capital * pos_size) * (pnl_pct / 100), 0),
                "exit_reason": exit_reason,
            })
            in_position = False
    return trades

# -----------------------------------------------------------------------
# 全体集計
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades: return {"total_trades": 0, "win_rate": 0.0, "avg_pnl_pct": 0.0, "total_pnl_yen": 0.0, "max_drawdown_pct": 0.0, "profit_factor": 0.0}
    df = pd.DataFrame(trades)
    wins, losses = df[df["pnl_pct"] > 0], df[df["pnl_pct"] <= 0]
    total_pnl = df["pnl_yen"].sum()
    pf = (wins["pnl_yen"].sum() / abs(losses["pnl_yen"].sum())) if not losses.empty else 1.0
    cumulative = df["pnl_yen"].cumsum()
    max_dd = (cumulative - cumulative.cummax()).min()
    return {
        "total_trades": len(df), "win_rate": round(len(wins)/len(df)*100, 1),
        "avg_pnl_pct": round(df["pnl_pct"].mean(), 2), "total_pnl_yen": round(total_pnl, 0),
        "max_drawdown_pct": round(max_dd / bt_params["initial_capital"] * 100, 2),
        "profit_factor": round(pf, 2),
    }

# -----------------------------------------------------------------------
# Gemini によるレポート整形
# -----------------------------------------------------------------------
def _format_report_with_gemini(summary: dict, all_trades: list[dict]) -> str:
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key: return _format_report_plain(summary)

    sorted_trades = sorted(all_trades, key=lambda x: x["pnl_pct"], reverse=True)
    top_5, worst_5 = sorted_trades[:5], sorted_trades[-5:]

    def _fmt_list(trades):
        return "\n".join([f"- {t['ticker']}: {t['pnl_pct']:+.1f}% ({t['entry_date']}～{t['exit_date']}, {t['exit_reason']})" for t in trades])

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")
    prompt = f"""
システムトレードのバックテスト結果(直近2年)を要約し、Discord用の投資レポートを作成してください。

【統計データ】
・総トレード数: {summary['total_trades']}回
・勝率: {summary['win_rate']}%
・合計損益: {summary['total_pnl_yen']:,}円
・プロフィットファクター: {summary['profit_factor']}
・最大ドローダウン: {summary['max_drawdown_pct']}%

【代表的なトレード例】
勝トレード上位:
{_fmt_list(top_5)}

負トレード下位:
{_fmt_list(worst_5)}

【依頼】
1. この数値から、現在の戦略が「どのような相場に強く、何が課題か」を200文字程度で客観的に分析してください。
2. 最後に、今後の改善に向けた一言アドバイスを添えてください。
3. 絵文字を使い、Markdown形式で出力してください。
※投資助言や将来予測は含めないでください。
"""
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Gemini 分析エラー: {e}")
        return _format_report_plain(summary) + "\n(※AI分析スキップ)"

def _format_report_plain(summary: dict) -> str:
    return (f"📊 **【バックテスト結果】**\n"
            f"総数: {summary['total_trades']} 回 / 勝率: {summary['win_rate']} %\n"
            f"損益: {summary['total_pnl_yen']:+,.0f} 円 / PF: {summary['profit_factor']}\n")

def _send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url: return
    for i in range(0, len(content), 1990):
        requests.post(url, json={"content": content[i : i + 1990]})

# -----------------------------------------------------------------------
# メイン
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始 (2年分データ)...")
    cfg, bt_params = _load_config(), _load_bt_params()
    db = DBManager()
    df = db.load_analysis_data(days=365 * 2) # 直近2年分

    if df.empty:
        print("⚠️ データ不足"); return

    tickers, all_trades = df["ticker"].unique(), []
    for ticker in tickers:
        df_ticker = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        all_trades.extend(_backtest_ticker(ticker, df_ticker, cfg, bt_params))

    summary = _calc_summary(all_trades, bt_params)
    report = _format_report_with_gemini(summary, all_trades)
    _send_discord("📈 **【バックテストレポート】**\n" + report)
    print("✅ 完了")

if __name__ == "__main__":
    run_backtest_and_report()
