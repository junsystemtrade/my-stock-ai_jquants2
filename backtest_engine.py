"""
backtest_engine.py
==================
signal_engine.py と同じシグナル条件（signals_config.yml）を使って
過去データでバックテストを行い、戦略の健全性を検証する。

Gemini SDK: google-generativeai（旧）→ google-genai（新・公式）に移行
"""

import os
import requests
import pandas as pd
from google import genai

from database_manager import DBManager
from signal_engine import _check_signals, _load_config


# -----------------------------------------------------------------------
# バックテストパラメータ
# -----------------------------------------------------------------------
_DEFAULT_BT_PARAMS = {
    "hold_days":       10,
    "stop_loss_pct":    5.0,
    "initial_capital":  1_000_000,
    "position_size":    0.1,
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
            if not hits:
                continue
            if i + 1 >= len(df):
                continue

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

            trade_capital = capital * pos_size
            pnl_pct       = (exit_price - entry_price) / entry_price * 100
            pnl_yen       = trade_capital * (pnl_pct / 100)

            trades.append({
                "ticker":      ticker,
                "signal_type": signal_type,
                "entry_date":  str(entry_date),
                "entry_price": round(entry_price, 2),
                "exit_date":   str(exit_date),
                "exit_price":  round(exit_price, 2),
                "pnl_pct":     round(pnl_pct, 2),
                "pnl_yen":     round(pnl_yen, 0),
                "exit_reason": exit_reason,
            })
            in_position = False

    return trades


# -----------------------------------------------------------------------
# 全体集計
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades:
        return {
            "total_trades": 0, "win_rate": 0.0, "avg_pnl_pct": 0.0,
            "total_pnl_yen": 0.0, "max_drawdown_pct": 0.0, "profit_factor": 0.0,
        }

    df      = pd.DataFrame(trades)
    wins    = df[df["pnl_pct"] > 0]
    losses  = df[df["pnl_pct"] <= 0]

    win_rate      = len(wins) / len(df) * 100
    avg_pnl_pct   = df["pnl_pct"].mean()
    total_pnl     = df["pnl_yen"].sum()
    gross_profit  = wins["pnl_yen"].sum() if not wins.empty else 0
    gross_loss    = abs(losses["pnl_yen"].sum()) if not losses.empty else 1e-9
    profit_factor = gross_profit / gross_loss

    cumulative = df["pnl_yen"].cumsum()
    peak       = cumulative.cummax()
    drawdown   = cumulative - peak
    max_dd_pct = drawdown.min() / bt_params["initial_capital"] * 100

    return {
        "total_trades":     len(df),
        "win_rate":         round(win_rate, 1),
        "avg_pnl_pct":      round(avg_pnl_pct, 2),
        "total_pnl_yen":    round(total_pnl, 0),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "profit_factor":    round(profit_factor, 2),
    }


# -----------------------------------------------------------------------
# Gemini によるサマリー整形（新 SDK 使用）
# -----------------------------------------------------------------------
def _format_report_with_gemini(summary: dict, top_trades: list[dict]) -> str:
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        return _format_report_plain(summary)

    # 新 SDK: google-genai
    client = genai.Client(api_key=api_key)

    top_str = "\n".join(
        f"  {t['ticker']} | {t['signal_type']} | {t['entry_date']}→{t['exit_date']}"
        f" | {t['pnl_pct']:+.1f}% ({t['exit_reason']})"
        for t in top_trades[:5]
    )

    prompt = f"""以下はシステムトレードのバックテスト結果です。
この数値を元に、Discord 向けの投資レポートを日本語で作成してください。

【制約】
- 売買の推奨・将来の予測は一切しないこと
- 数値をそのまま読み上げるのではなく、戦略の特徴を分かりやすく説明すること
- 800文字以内・絵文字を適度に使って読みやすく

【バックテスト結果】
総トレード数  : {summary['total_trades']} 回
勝率          : {summary['win_rate']} %
平均損益      : {summary['avg_pnl_pct']:+.2f} %
合計損益      : {summary['total_pnl_yen']:+,.0f} 円
最大ドローダウン: {summary['max_drawdown_pct']:.2f} %
プロフィットファクター: {summary['profit_factor']:.2f}

【直近トレード例（上位5件）】
{top_str}"""

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Gemini レポート生成失敗: {e}")
        return _format_report_plain(summary)


def _format_report_plain(summary: dict) -> str:
    return (
        f"📊 **【バックテスト結果】**\n"
        f"総トレード数: {summary['total_trades']} 回\n"
        f"勝率: {summary['win_rate']} %\n"
        f"平均損益: {summary['avg_pnl_pct']:+.2f} %\n"
        f"合計損益: {summary['total_pnl_yen']:+,.0f} 円\n"
        f"最大ドローダウン: {summary['max_drawdown_pct']:.2f} %\n"
        f"プロフィットファクター: {summary['profit_factor']:.2f}\n"
    )


# -----------------------------------------------------------------------
# Discord 送信
# -----------------------------------------------------------------------
def _send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ DISCORD_WEBHOOK_URL が未設定です。スキップ。")
        return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        res   = requests.post(url, json={"content": chunk})
        if res.status_code not in (200, 204):
            print(f"❌ Discord 送信エラー: {res.status_code}")


# -----------------------------------------------------------------------
# 公開インターフェース
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始...")

    cfg       = _load_config()
    bt_params = _load_bt_params()

    db = DBManager()
    df = db.load_analysis_data(days=365 * 3)

    if df.empty:
        print("⚠️ データが不足しています。バックテストをスキップします。")
        return

    tickers    = df["ticker"].unique()
    all_trades = []

    print(f"🔁 {len(tickers):,} 銘柄でバックテスト実行中...")

    for ticker in tickers:
        df_ticker = (
            df[df["ticker"] == ticker]
            .sort_values("date")
            .reset_index(drop=True)
        )
        trades = _backtest_ticker(ticker, df_ticker, cfg, bt_params)
        all_trades.extend(trades)

    print(f"✅ バックテスト完了: {len(all_trades)} トレード")

    if not all_trades:
        msg = "📊 バックテスト完了：条件に合致するトレードがありませんでした。"
        print(msg)
        _send_discord(msg)
        return

    # --- 【ここから追加：銘柄別パフォーマンス分析】 ---
    trades_df = pd.DataFrame(all_trades)
    
    # 銘柄ごとに損益を合計・平均・回数で集計
    ticker_stats = trades_df.groupby('ticker')['pnl_pct'].agg(['sum', 'mean', 'count']).reset_index()
    
    # 利益貢献トップ10
    top_10 = ticker_stats.sort_values(by='sum', ascending=False).head(10)
    # 損失ワースト10
    worst_10 = ticker_stats.sort_values(by='sum', ascending=True).head(10)

    print("\n" + "="*30)
    print("🔥 利益貢献トップ10銘柄 (合計損益順)")
    print(top_10.to_string(index=False))
    print("\n💀 損失ワースト10銘柄 (合計損益順)")
    print(worst_10.to_string(index=False))
    print("="*30 + "\n")
    # --- 【ここまで追加】 ---

    summary = _calc_summary(all_trades, bt_params)
    
    # (以下、既存のレポート生成と送信処理)
    top_trades = sorted(all_trades, key=lambda x: x["pnl_pct"], reverse=True)
    report = _format_report_with_gemini(summary, top_trades)
    
    _send_discord("📈 **【バックテストレポート】**\n" + report)

    summary    = _calc_summary(all_trades, bt_params)
    top_trades = sorted(all_trades, key=lambda x: x["pnl_pct"], reverse=True)
    report     = _format_report_with_gemini(summary, top_trades)

    _send_discord("📈 **【バックテストレポート】**\n" + report)
    print("\n" + _format_report_plain(summary))


if __name__ == "__main__":
    run_backtest_and_report()
