"""
backtest_engine.py
==================
signal_engine.py と同じシグナル条件（signals_config.yml）を使って
過去データでバックテストを行い、戦略の健全性を検証する。

設計方針:
  - シグナル判定は signal_engine._check_signals() を直接再利用
    → シグナルエンジンとバックテストが常に同一条件で動く
  - エントリー・エグジットはコードで計算（AI は行わない）
  - Gemini はバックテスト結果のサマリーを Discord 向けに整形するだけ

バックテストルール:
  - エントリー : シグナル発生翌日の始値で買い
  - エグジット : 以下のいずれか早い方
      (a) 保有 hold_days 日後の終値
      (b) ストップロス stop_loss_pct % 下落時の終値
  - 同一銘柄の重複エントリーなし（保有中は再エントリーしない）
"""

import os
import requests
import pandas as pd
import google.generativeai as genai

from database_manager import DBManager
# signal_engine のシグナル判定ロジックをそのまま再利用
from signal_engine import _check_signals, _load_config


# -----------------------------------------------------------------------
# バックテストパラメータ（signals_config.yml の backtest セクションで上書き可）
# -----------------------------------------------------------------------
_DEFAULT_BT_PARAMS = {
    "hold_days":      10,    # 最大保有日数
    "stop_loss_pct":   5.0,  # ストップロス（%）
    "initial_capital": 1_000_000,  # 初期資金（円）
    "position_size":   0.1,  # 1 回のエントリーに使う資金割合（10%）
}


def _load_bt_params() -> dict:
    cfg = _load_config()
    bt  = cfg.get("backtest", {})
    return {**_DEFAULT_BT_PARAMS, **bt}


# -----------------------------------------------------------------------
# 1 銘柄のバックテスト
# -----------------------------------------------------------------------
def _backtest_ticker(
    ticker: str,
    df: pd.DataFrame,
    cfg: dict,
    bt_params: dict,
) -> list[dict]:
    """
    1 銘柄の全期間でシグナルを検出し、エントリー〜エグジットの損益を計算する。

    Returns
    -------
    list[dict]
        各トレードの結果。各要素:
        {
            ticker, signal_type, entry_date, entry_price,
            exit_date, exit_price, pnl_pct, pnl_yen, exit_reason
        }
    """
    hold_days   = bt_params["hold_days"]
    stop_loss   = bt_params["stop_loss_pct"] / 100
    pos_size    = bt_params["position_size"]
    capital     = bt_params["initial_capital"]

    trades      = []
    in_position = False
    min_days    = cfg.get("filter", {}).get("min_data_days", 80)

    for i in range(min_days, len(df)):
        # シグナル判定は i 日目までのデータで行う
        window_df = df.iloc[: i + 1].copy()

        if not in_position:
            hits = _check_signals(ticker, window_df, cfg)
            if not hits:
                continue

            # エントリー: 翌日の始値（i+1 日目）
            if i + 1 >= len(df):
                continue

            entry_row   = df.iloc[i + 1]
            entry_date  = entry_row["date"]
            entry_price = float(entry_row["open"]) if pd.notna(entry_row["open"]) else float(entry_row["price"])
            signal_type = hits[0]["signal_type"]   # 複数シグナルは最初を代表として使用
            in_position = True

            # エグジット探索
            exit_price  = None
            exit_date   = None
            exit_reason = "期間満了"

            for j in range(i + 2, min(i + 2 + hold_days, len(df))):
                row      = df.iloc[j]
                close    = float(row["price"])
                pnl_rate = (close - entry_price) / entry_price

                # ストップロス判定
                if pnl_rate <= -stop_loss:
                    exit_price  = close
                    exit_date   = row["date"]
                    exit_reason = f"ストップロス({stop_loss*100:.1f}%)"
                    break

            # ストップロスにかからなければ hold_days 後の終値で手仕舞い
            if exit_price is None:
                exit_idx    = min(i + 1 + hold_days, len(df) - 1)
                exit_price  = float(df.iloc[exit_idx]["price"])
                exit_date   = df.iloc[exit_idx]["date"]

            # 損益計算
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

            in_position = False   # 次のシグナルを受け付ける

    return trades


# -----------------------------------------------------------------------
# 全体集計
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    """
    全トレードから主要指標を計算する。
    """
    if not trades:
        return {
            "total_trades":    0,
            "win_rate":        0.0,
            "avg_pnl_pct":     0.0,
            "total_pnl_yen":   0.0,
            "max_drawdown_pct": 0.0,
            "profit_factor":   0.0,
        }

    df      = pd.DataFrame(trades)
    wins    = df[df["pnl_pct"] > 0]
    losses  = df[df["pnl_pct"] <= 0]

    win_rate    = len(wins) / len(df) * 100
    avg_pnl_pct = df["pnl_pct"].mean()
    total_pnl   = df["pnl_yen"].sum()

    gross_profit = wins["pnl_yen"].sum() if not wins.empty else 0
    gross_loss   = abs(losses["pnl_yen"].sum()) if not losses.empty else 1e-9
    profit_factor = gross_profit / gross_loss

    # 最大ドローダウン（累積損益ベース）
    cumulative  = df["pnl_yen"].cumsum()
    peak        = cumulative.cummax()
    drawdown    = (cumulative - peak)
    max_dd_yen  = drawdown.min()
    capital     = bt_params["initial_capital"]
    max_dd_pct  = max_dd_yen / capital * 100 if capital > 0 else 0.0

    return {
        "total_trades":     len(df),
        "win_rate":         round(win_rate, 1),
        "avg_pnl_pct":      round(avg_pnl_pct, 2),
        "total_pnl_yen":    round(total_pnl, 0),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "profit_factor":    round(profit_factor, 2),
    }


# -----------------------------------------------------------------------
# Gemini によるサマリー整形（Discord 向け）
# -----------------------------------------------------------------------
def _format_report_with_gemini(summary: dict, top_trades: list[dict]) -> str:
    """
    バックテストの数値サマリーを Gemini に渡し、
    Discord 向けの読みやすいレポート文章に整形させる。
    ※ 売買推奨・将来予測は一切させない。
    """
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        return _format_report_plain(summary)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")

    top_str = "\n".join(
        f"  {t['ticker']} | {t['signal_type']} | {t['entry_date']}→{t['exit_date']}"
        f" | {t['pnl_pct']:+.1f}% ({t['exit_reason']})"
        for t in top_trades[:5]
    )

    prompt = f"""
以下はシステムトレードのバックテスト結果です。
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
{top_str}
"""

    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Gemini レポート生成失敗: {e}")
        return _format_report_plain(summary)


def _format_report_plain(summary: dict) -> str:
    """Gemini が使えない場合のプレーンテキストレポート"""
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
        print("⚠️ DISCORD_WEBHOOK_URL が未設定です。Discord 送信をスキップ。")
        return
    # Discord の上限 2000 文字に合わせて分割送信
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        res   = requests.post(url, json={"content": chunk})
        if res.status_code not in (200, 204):
            print(f"❌ Discord 送信エラー: {res.status_code}")


# -----------------------------------------------------------------------
# 公開インターフェース
# -----------------------------------------------------------------------
def run_backtest_and_report():
    """
    DB から全期間データをロードし、全銘柄でバックテストを実行。
    結果サマリーを Gemini で整形して Discord に送信する。
    """
    print("📊 バックテスト開始...")

    # 設定読み込み
    cfg       = _load_config()
    bt_params = _load_bt_params()

    # DB からデータロード（バックテストは全期間 = days=365*5）
    db = DBManager()
    df = db.load_analysis_data(days=365 * 5)

    if df.empty:
        print("⚠️ データが不足しています。バックテストをスキップします。")
        return

    tickers     = df["ticker"].unique()
    all_trades  = []

    print(f"🔁 {len(tickers)} 銘柄でバックテスト実行中...")

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

    # 集計
    summary = _calc_summary(all_trades, bt_params)

    # 損益上位トレードを抽出（Discord レポート用）
    top_trades = sorted(all_trades, key=lambda x: x["pnl_pct"], reverse=True)

    # Gemini でレポート整形
    report = _format_report_with_gemini(summary, top_trades)
    header = "📈 **【バックテストレポート】**\n"
    _send_discord(header + report)

    # コンソールにも出力
    print("\n" + header + _format_report_plain(summary))
    print(f"  上位トレード例: {top_trades[0]['ticker']} {top_trades[0]['pnl_pct']:+.1f}%")


if __name__ == "__main__":
    run_backtest_and_report()
