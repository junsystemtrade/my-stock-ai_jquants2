"""
weekly_report.py
================
週次トレード成績レポートを集計してDiscordに通知する。
毎週土曜日に自動実行。

変更点:
  - 銘柄名をJPXマスターから取得して通知に追加
"""

import os
import requests
import pandas as pd
from database_manager import DBManager
import portfolio_manager


def _send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ Discord Webhook URL 未設定")
        return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        try:
            requests.post(url, json={"content": chunk})
        except Exception as e:
            print(f"❌ Discord送信失敗: {e}")


def _load_ticker_names() -> dict:
    """JPXマスターから {コード: 銘柄名} の辞書を返す。"""
    try:
        stock_map = portfolio_manager.get_target_tickers()
        return {
            k.replace(".T", ""): v["name"]
            for k, v in stock_map.items()
        }
    except Exception as e:
        print(f"⚠️ 銘柄名取得失敗: {e}")
        return {}


def _get_company_name(code: str, names: dict) -> str:
    """銘柄コードから銘柄名を返す。取得できない場合はコードをそのまま返す。"""
    return names.get(code.replace(".T", ""), code)


def _calc_stats(df: pd.DataFrame) -> dict:
    """トレード結果を集計する。"""
    if df.empty:
        return {}

    df = df.copy()
    df["pnl_pct"] = (df["exit_price"] - df["entry_price"]) / df["entry_price"] * 100

    wins   = df[df["pnl_pct"] > 0]
    losses = df[df["pnl_pct"] <= 0]

    gross_profit = wins["pnl_pct"].sum()
    gross_loss   = abs(losses["pnl_pct"].sum()) or 1e-9

    return {
        "total":         len(df),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(len(wins) / len(df) * 100, 1),
        "avg_pnl":       round(df["pnl_pct"].mean(), 2),
        "total_pnl":     round(df["pnl_pct"].sum(), 2),
        "best":          df.loc[df["pnl_pct"].idxmax()],
        "worst":         df.loc[df["pnl_pct"].idxmin()],
        "profit_factor": round(gross_profit / gross_loss, 2),
    }


def _format_stats_block(label: str, stats: dict, names: dict) -> str:
    """集計結果をDiscord用テキストに整形する。"""
    if not stats:
        return f"**{label}**\nデータなし\n"

    win_emoji  = "🟢" if stats["win_rate"] >= 50 else "🔴"
    pnl_emoji  = "📈" if stats["total_pnl"] >= 0 else "📉"
    best       = stats["best"]
    worst      = stats["worst"]
    best_name  = _get_company_name(best["ticker"], names)
    worst_name = _get_company_name(worst["ticker"], names)

    return (
        f"**{label}**\n"
        f"総トレード: {stats['total']}回 "
        f"（勝: {stats['wins']} / 負: {stats['losses']}）\n"
        f"{win_emoji} 勝率: {stats['win_rate']}%\n"
        f"平均損益: {stats['avg_pnl']:+.2f}%\n"
        f"{pnl_emoji} 合計損益: {stats['total_pnl']:+.2f}%\n"
        f"PF: {stats['profit_factor']}\n"
        f"🏆 最大利益: **{best_name}**（{best['ticker']}）"
        f" {best['pnl_pct']:+.2f}% / {best['close_reason']}\n"
        f"💔 最大損失: **{worst_name}**（{worst['ticker']}）"
        f" {worst['pnl_pct']:+.2f}% / {worst['close_reason']}\n"
    )


def run_weekly_report():
    print("📊 週次レポート生成開始...")
    db    = DBManager()
    names = _load_ticker_names()

    # 今週のトレード
    weekly_df    = db.load_weekly_trades()
    weekly_stats = _calc_stats(weekly_df)

    # 累計トレード
    all_df    = db.load_all_closed_trades()
    all_stats = _calc_stats(all_df)

    # 今週の個別トレード一覧
    trade_list = ""
    if not weekly_df.empty:
        weekly_df = weekly_df.copy()
        weekly_df["pnl_pct"] = (
            (weekly_df["exit_price"] - weekly_df["entry_price"])
            / weekly_df["entry_price"] * 100
        )
        trade_list  = "\n**【今週の個別トレード】**\n"
        trade_list += "─" * 20 + "\n"
        for _, t in weekly_df.iterrows():
            emoji   = "📈" if t["pnl_pct"] >= 0 else "📉"
            company = _get_company_name(t["ticker"], names)
            trade_list += (
                f"{emoji} **{company}**（{t['ticker']}）\n"
                f"　買値: {int(t['entry_price']):,}円 → "
                f"売値: {int(t['exit_price']):,}円 "
                f"({t['pnl_pct']:+.2f}%)\n"
                f"　理由: {t['close_reason']}\n"
                f"　期間: {t['entry_date']} → {t['closed_date']}\n"
            )

    # レポート組み立て
    report  = "📅 **【週次トレード成績レポート】**\n"
    report += "━" * 20 + "\n"
    report += _format_stats_block("📊 今週の成績", weekly_stats, names)
    report += "━" * 20 + "\n"
    report += _format_stats_block("📈 累計成績", all_stats, names)
    report += trade_list

    _send_discord(report)
    print("✅ 週次レポート送信完了")


if __name__ == "__main__":
    run_weekly_report()
