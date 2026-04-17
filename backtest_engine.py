"""
backtest_engine.py
==================
市場環境（地合い）とスコアリングを用いた精鋭選別バックテスト。
トレーリング保有延長・ストップ高除外に対応。
RSI過熱の集計を正規化し、通知を2000文字以内に収める。

変更点:
  - stop_loss_pct: 5.0 -> 3.0 (signals_config.ymlで設定)
  - exclude_score_range: [80, 85] で80-84.9点帯を除外
"""

import os
import re
import requests
import pandas as pd
from google import genai

from database_manager import DBManager
from signal_engine import _check_signals, _load_config, _is_stop_high, _calculate_indicators
from scoring_system import calculate_score

# -----------------------------------------------------------------------
# バックテストパラメータ
# -----------------------------------------------------------------------
_DEFAULT_BT_PARAMS = {
    "stop_loss_pct":       5.0,
    "initial_capital":     1_000_000,
    "position_size":       0.1,
    "max_daily_entries":   3,
    "market_crash_limit":  -2.0,
}

def _load_bt_params() -> dict:
    cfg = _load_config()
    bt  = cfg.get("backtest", {})
    return {**_DEFAULT_BT_PARAMS, **bt}


# -----------------------------------------------------------------------
# スコア除外判定
# -----------------------------------------------------------------------
def _is_score_excluded(score: float, bt_params: dict) -> bool:
    excl = bt_params.get("exclude_score_range")
    if excl and len(excl) == 2:
        if excl[0] <= score < excl[1]:
            return True
    return False


# -----------------------------------------------------------------------
# RSI過熱理由の正規化
# -----------------------------------------------------------------------
def _normalize_exit_reason(reason: str) -> str:
    return re.sub(r"RSI過熱\([\d.]+\)", "RSI過熱", reason)


# -----------------------------------------------------------------------
# トレーリング手じまい判定
# -----------------------------------------------------------------------
def _should_exit_trailing(row: pd.Series, entry_price: float, cfg: dict) -> str | None:
    trailing_cfg = cfg.get("exit_rules", {}).get("trailing", {})
    trail_cond   = trailing_cfg.get("conditions", {})
    rsi_limit    = cfg.get("exit_rules", {}).get("immediate", {}).get("rsi_overbought", 70)

    current_price = float(row["price"])
    pnl_pct       = (current_price - entry_price) / entry_price * 100
    rsi           = float(row.get("rsi_14", 50))
    sma_5         = float(row.get("sma_5",  0))
    sma_25        = float(row.get("sma_25", 0))
    is_gc         = sma_5 > sma_25

    reasons = []
    if trail_cond.get("golden_cross_maintained", True) and not is_gc:
        reasons.append("5日線<25日線")
    if rsi >= trail_cond.get("rsi_below", rsi_limit):
        reasons.append("RSI過熱")
    if trail_cond.get("profit_required", True) and pnl_pct <= 0:
        reasons.append("含み損転落")

    if reasons:
        return f"トレーリング手じまい（{' / '.join(reasons)}）"
    return None


# -----------------------------------------------------------------------
# 単一トレードのシミュレーション
# -----------------------------------------------------------------------
def _execute_trade(sig: dict, bt_params: dict, cfg: dict) -> dict | None:
    df        = sig["df_ticker"]
    idx       = sig["entry_idx"]
    stop_loss = bt_params["stop_loss_pct"] / 100
    hold_days = cfg.get("exit_rules", {}).get("hold_days", 10)

    entry_row   = df.iloc[idx]
    entry_price = (
        float(entry_row["open"])
        if pd.notna(entry_row.get("open")) and entry_row["open"] > 0
        else float(entry_row["price"])
    )
    if entry_price <= 0:
        return None

    exit_price, exit_date, exit_reason = None, None, "期間満了"

    for j in range(idx + 1, len(df)):
        curr_row  = df.iloc[j]
        low_price = (
            float(curr_row["low"])
            if pd.notna(curr_row.get("low"))
            else float(curr_row["price"])
        )
        held = j - idx

        # ストップロス
        if (low_price - entry_price) / entry_price <= -stop_loss:
            exit_price  = entry_price * (1 - stop_loss)
            exit_date   = curr_row["date"]
            exit_reason = "ストップロス"
            break

        # 即時手じまい（デッドクロス・RSI過熱）
        sma_s_prev = float(df.iloc[j-1].get("sma_5",  0)) if j > 0 else 0
        sma_l_prev = float(df.iloc[j-1].get("sma_25", 0)) if j > 0 else 0
        sma_s_curr = float(curr_row.get("sma_5",  0))
        sma_l_curr = float(curr_row.get("sma_25", 0))
        rsi_curr   = float(curr_row.get("rsi_14", 50))

        is_dc = sma_s_prev >= sma_l_prev and sma_s_curr < sma_l_curr
        if is_dc:
            exit_price  = float(curr_row["price"])
            exit_date   = curr_row["date"]
            exit_reason = "デッドクロス"
            break
        if rsi_curr >= cfg.get("exit_rules", {}).get("immediate", {}).get("rsi_overbought", 70):
            exit_price  = float(curr_row["price"])
            exit_date   = curr_row["date"]
            exit_reason = "RSI過熱"
            break

        # hold_days経過後: トレーリング判定（毎日）
        if held >= hold_days:
            trail_reason = _should_exit_trailing(curr_row, entry_price, cfg)
            if trail_reason:
                exit_price  = float(curr_row["price"])
                exit_date   = curr_row["date"]
                exit_reason = trail_reason
                break

    if exit_price is None:
        exit_row    = df.iloc[-1]
        exit_price  = (
            float(exit_row["open"])
            if pd.notna(exit_row.get("open")) and exit_row["open"] > 0
            else float(exit_row["price"])
        )
        exit_date   = exit_row["date"]
        exit_reason = "データ末尾"

    pnl_pct    = (exit_price - entry_price) / entry_price * 100
    pnl_yen    = (bt_params["initial_capital"] * bt_params["position_size"]) * (pnl_pct / 100)
    held_total = (pd.to_datetime(exit_date) - pd.to_datetime(entry_row["date"])).days

    return {
        "ticker":      sig["ticker"],
        "score":       sig["score"],
        "signal_type": sig["signal_type"],
        "entry_date":  entry_row["date"],
        "entry_price": round(entry_price, 2),
        "exit_date":   exit_date,
        "exit_price":  round(exit_price, 2),
        "pnl_pct":     round(pnl_pct, 2),
        "pnl_yen":     round(pnl_yen, 0),
        "exit_reason": exit_reason,
        "held_days":   held_total,
    }


# -----------------------------------------------------------------------
# 集計・レポート
# -----------------------------------------------------------------------
def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades:
        return {"total_trades": 0}

    df = pd.DataFrame(trades)
    df["score_bin"] = (df["score"] // 5) * 5
    df["exit_reason_normalized"] = df["exit_reason"].apply(_normalize_exit_reason)

    score_stats = {}
    for bin_val, group in df.groupby("score_bin"):
        wins         = group[group["pnl_pct"] > 0]
        losses       = group[group["pnl_pct"] <= 0]
        gross_profit = wins["pnl_yen"].sum()
        gross_loss   = abs(losses["pnl_yen"].sum()) or 1e-9
        score_stats[bin_val] = {
            "count":      len(group),
            "win_rate":   round(len(wins) / len(group) * 100, 1),
            "avg_return": round(group["pnl_pct"].mean(), 2),
            "avg_held":   round(group["held_days"].mean(), 1),
            "pf":         round(gross_profit / gross_loss, 2),
        }

    wins         = df[df["pnl_pct"] > 0]
    losses       = df[df["pnl_pct"] <= 0]
    gross_profit = wins["pnl_yen"].sum()
    gross_loss   = abs(losses["pnl_yen"].sum()) or 1e-9
    cumulative   = df["pnl_yen"].cumsum()
    max_dd       = (cumulative - cumulative.cummax()).min()
    exit_counts  = df["exit_reason_normalized"].value_counts().to_dict()

    return {
        "total_trades":     len(df),
        "win_rate":         round(len(wins) / len(df) * 100, 1),
        "avg_pnl_pct":      round(df["pnl_pct"].mean(), 2),
        "avg_held_days":    round(df["held_days"].mean(), 1),
        "total_pnl_yen":    round(df["pnl_yen"].sum(), 0),
        "max_drawdown_pct": round(max_dd / bt_params["initial_capital"] * 100, 2),
        "profit_factor":    round(gross_profit / gross_loss, 2),
        "exit_counts":      exit_counts,
        "score_analysis":   score_stats,
    }


def _format_report_plain(summary: dict) -> str:
    if not summary.get("total_trades"):
        return "📊 トレードなし"

    exit_str = "\n【手じまい理由の内訳】\n"
    for reason, count in sorted(
        summary.get("exit_counts", {}).items(), key=lambda x: -x[1]
    )[:10]:
        exit_str += f"  {reason}: {count}回\n"

    score_str = "\n【スコア別詳細分析（5点刻み）】\n"
    score_str += "─" * 40 + "\n"
    for bin_val, v in sorted(
        summary.get("score_analysis", {}).items(), key=lambda x: x[0], reverse=True
    )[:10]:
        label = f"{bin_val:2.0f}-{bin_val+4.9:4.1f}点"
        score_str += (
            f"{label}: {v['count']:>3}回 | 勝率{v['win_rate']:>5}% | "
            f"平均{v['avg_return']:>+6.2f}% | 保有{v['avg_held']:>5.1f}日 | PF:{v['pf']:>4.2f}\n"
        )

    return (
        f"📊 **バックテスト結果(精鋭+トレーリングモード)**\n"
        f"総数: {summary['total_trades']}回 / 勝率: {summary['win_rate']}%\n"
        f"平均損益: {summary['avg_pnl_pct']:+.2f}% / 平均保有: {summary['avg_held_days']:.1f}日\n"
        f"合計: {summary['total_pnl_yen']:+,.0f}円\n"
        f"PF: {summary['profit_factor']} / 最大DD: {summary['max_drawdown_pct']}%\n"
        f"{exit_str}"
        f"{score_str}"
    )


def _format_report_with_gemini(summary: dict, top_trades: list[dict]) -> str:
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        return _format_report_plain(summary)
    client = genai.Client(api_key=api_key)
    prompt = (
        "日本株バックテスト結果です。トレーリング保有延長を導入した結果として、"
        "スコア帯・手じまい理由・平均保有日数の観点から改善案を300字以内で要約してください。\n\n"
        f"結果:\n{summary}\n\n上位トレード:\n{top_trades[:3]}"
    )
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash", contents=prompt
        )
        gemini_text = response.text.strip()
        return f"{_format_report_plain(summary)}\n\n💡 **Gemini考察**\n{gemini_text}"
    except Exception as e:
        print(f"⚠️ Gemini分析失敗: {e}")
        return _format_report_plain(summary)


def _send_discord(content: str):
    if os.getenv("NOTIFY_DISCORD", "true").lower() == "false":
        print("📭 Discord通知はスキップされました（NOTIFY_DISCORD=false）")
        return
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        return
    for i in range(0, len(content), 1990):
        requests.post(url, json={"content": content[i: i + 1990]})


# -----------------------------------------------------------------------
# メイン実行
# -----------------------------------------------------------------------
def run_backtest_and_report():
    print("📊 バックテスト開始（トレーリングモード）...")
    cfg       = _load_config()
    bt_params = _load_bt_params()

    excl = bt_params.get("exclude_score_range")
    print(f"  ストップロス: {bt_params['stop_loss_pct']}%")
    if excl:
        print(f"  除外スコア帯: {excl[0]}〜{excl[1]-0.1:.1f}点")

    db     = DBManager()
    df_all = db.load_analysis_data(days=365 * 3)
    if df_all.empty:
        return

    niy_df = df_all[df_all["ticker"] == "NIY=F"].sort_values("date").copy()
    niy_df["m_change"] = niy_df["price"].pct_change() * 100
    crash_dates = set(niy_df[niy_df["m_change"] <= bt_params["market_crash_limit"]]["date"])

    all_signals = []
    tickers     = [t for t in df_all["ticker"].unique() if t != "NIY=F"]
    min_days    = cfg.get("filter", {}).get("min_data_days", 80)

    for ticker in tickers:
        df_ticker = (
            df_all[df_all["ticker"] == ticker]
            .sort_values("date")
            .reset_index(drop=True)
        )
        if len(df_ticker) < min_days:
            continue

        df_ticker = _calculate_indicators(df_ticker)

        for i in range(min_days, len(df_ticker) - 1):
            entry_date = df_ticker.iloc[i + 1]["date"]
            if entry_date in crash_dates:
                continue

            stop_high_cfg = cfg.get("filter", {}).get("stop_high", {})
            if stop_high_cfg.get("enabled", True) and i >= 1:
                curr_p = float(df_ticker.iloc[i]["price"])
                prev_p = float(df_ticker.iloc[i - 1]["price"])
                if _is_stop_high(curr_p, prev_p):
                    continue

            hits = _check_signals(ticker, df_ticker.iloc[: i + 1], cfg)
            if not hits:
                continue

            score = calculate_score(
                pd.Series(hits[0]), cfg.get("scoring_logic", {})
            )

            # 80〜84.9点帯を除外
            if _is_score_excluded(score, bt_params):
                continue

            all_signals.append({
                "date":        pd.to_datetime(entry_date),
                "ticker":      ticker,
                "score":       score,
                "signal_type": hits[0]["signal_type"],
                "df_ticker":   df_ticker,
                "entry_idx":   i + 1,
            })

    if not all_signals:
        print("シグナルが検出されませんでした。")
        return

    sig_df   = pd.DataFrame(all_signals)
    selected = (
        sig_df
        .sort_values(["date", "score"], ascending=[True, False])
        .groupby("date")
        .head(bt_params["max_daily_entries"])
    )

    final_trades, free_dates = [], {}

    for _, sig in selected.iterrows():
        ticker = sig["ticker"]
        if ticker in free_dates and sig["date"] < pd.to_datetime(free_dates[ticker]):
            continue
        trade = _execute_trade(sig, bt_params, cfg)
        if trade:
            final_trades.append(trade)
            free_dates[ticker] = trade["exit_date"]

    if final_trades:
        summary = _calc_summary(final_trades, bt_params)
        report  = _format_report_with_gemini(
            summary,
            sorted(final_trades, key=lambda x: x["pnl_pct"], reverse=True)
        )
        _send_discord(f"📈 **精鋭バックテストレポート（トレーリングモード）**\n{report}")
        print(_format_report_plain(summary))
    else:
        print("有効なトレードがありませんでした。")


if __name__ == "__main__":
    run_backtest_and_report()
