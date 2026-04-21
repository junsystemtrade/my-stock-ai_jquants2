"""
backtest_engine.py (修正・完全版)

【修正内容】
- ImportError を解消（関数を同一ファイル内に配置）
- scan_signals の一括処理による高速化ロジックを維持
- 3年分のバックテストに対応
"""

import os
import re
import requests
import pandas as pd
from google import genai
from datetime import datetime

from database_manager import DBManager
from signal_engine import scan_signals, _load_config, _calculate_indicators
from scoring_system import calculate_score

# -----------------------------------------------------------------------
# バックテストパラメータ
# -----------------------------------------------------------------------

_DEFAULT_BT_PARAMS = {
    "stop_loss_pct":       3.0,
    "initial_capital":     1_000_000,
    "position_size":       0.1,
    "max_daily_entries":   2,
    "market_crash_limit":  -2.0,
    "min_score":           55.0,
}

def _load_bt_params() -> dict:
    cfg = _load_config()
    bt  = cfg.get("backtest", {})
    return {**_DEFAULT_BT_PARAMS, **bt}

def _get_open(row: pd.Series) -> float:
    o = row.get("open")
    if o is not None and pd.notna(o) and float(o) > 0:
        return float(o)
    return float(row["price"])

def _is_stop_high_internal(current_price: float, prev_price: float) -> bool:
    if prev_price <= 0: return False
    return (current_price / prev_price) >= 1.14

def _normalize_exit_reason(reason: str) -> str:
    return re.sub(r"RSI過熱([\d.]+)", "RSI過熱", reason)

# -----------------------------------------------------------------------
# 手じまい・ロジック
# -----------------------------------------------------------------------

def _should_exit_trailing(row, entry_price, cfg, held, hold_days):
    trailing_cfg  = cfg.get("exit_rules", {}).get("trailing", {})
    trail_cond    = trailing_cfg.get("conditions", {})
    rsi_limit     = cfg.get("exit_rules", {}).get("immediate", {}).get("rsi_overbought", 70)
    early_act_pct = float(trailing_cfg.get("early_activation_pct", 999))

    current_price = float(row["price"])
    pnl_pct       = (current_price - entry_price) / entry_price * 100
    rsi           = float(row.get("rsi_14", 50))
    sma_5         = float(row.get("sma_5",  0))
    sma_25        = float(row.get("sma_25", 0))

    if held < hold_days and pnl_pct < early_act_pct:
        return False, []

    reasons = []
    if trail_cond.get("golden_cross_maintained", True) and not (sma_5 > sma_25):
        reasons.append("5日線<25日線")
    if rsi >= trail_cond.get("rsi_below", rsi_limit):
        reasons.append("RSI過熱")
    if trail_cond.get("profit_required", True) and pnl_pct <= 0:
        reasons.append("含み損転落")

    return bool(reasons), reasons

def _execute_trade(sig: dict, bt_params: dict, cfg: dict) -> dict | None:
    df = sig["df_ticker"]
    idx = sig["entry_idx"]
    stop_loss = bt_params["stop_loss_pct"] / 100
    hold_days = cfg.get("exit_rules", {}).get("hold_days", 10)

    entry_row = df.iloc[idx]
    entry_price = _get_open(entry_row)
    if entry_price <= 0: return None

    pending_exit = None
    for j in range(idx + 1, len(df)):
        curr_row = df.iloc[j]
        if pending_exit:
            exit_p = _get_open(curr_row)
            return {
                "ticker": sig["ticker"], "score": sig["score"], "signal_type": sig["signal_type"],
                "entry_date": entry_row["date"], "entry_price": round(entry_price, 2),
                "exit_date": curr_row["date"], "exit_price": round(exit_p, 2),
                "pnl_pct": round(((exit_p - entry_price) / entry_price * 100), 2),
                "pnl_yen": round((bt_params["initial_capital"] * bt_params["position_size"]) * ((exit_p - entry_price) / entry_price), 0),
                "exit_reason": pending_exit, "held_days": (pd.to_datetime(curr_row["date"]) - pd.to_datetime(entry_row["date"])).days,
            }

        close_p = float(curr_row["price"])
        if (close_p - entry_price) / entry_price * 100 <= -(stop_loss * 100):
            pending_exit = "ストップロス"
        elif float(df.iloc[j-1].get("sma_5",0)) >= float(df.iloc[j-1].get("sma_25",0)) and \
             float(curr_row.get("sma_5",0)) < float(curr_row.get("sma_25",0)):
            pending_exit = "デッドクロス"
        elif float(curr_row.get("rsi_14", 50)) >= cfg.get("exit_rules",{}).get("immediate",{}).get("rsi_overbought", 70):
            pending_exit = "RSI過熱"
        else:
            triggered, reasons = _should_exit_trailing(curr_row, entry_price, cfg, j - idx, hold_days)
            if triggered: pending_exit = f"トレーリング（{' / '.join(reasons)}）"

    last_row = df.iloc[-1]
    return {
        "ticker": sig["ticker"], "score": sig["score"], "signal_type": sig["signal_type"],
        "entry_date": entry_row["date"], "entry_price": round(entry_price, 2),
        "exit_date": last_row["date"], "exit_price": round(_get_open(last_row), 2),
        "pnl_pct": round(((_get_open(last_row) - entry_price) / entry_price * 100), 2),
        "pnl_yen": round((bt_params["initial_capital"] * bt_params["position_size"]) * ((_get_open(last_row) - entry_price) / entry_price), 0),
        "exit_reason": pending_exit or "データ末尾", "held_days": (pd.to_datetime(last_row["date"]) - pd.to_datetime(entry_row["date"])).days,
    }

# -----------------------------------------------------------------------
# レポート作成用関数 (内部に移動)
# -----------------------------------------------------------------------

def _calc_summary(trades: list[dict], bt_params: dict) -> dict:
    if not trades: return {"total_trades": 0}
    df = pd.DataFrame(trades)
    wins = df[df["pnl_pct"] > 0]
    gross_p = wins["pnl_yen"].sum()
    gross_l = abs(df[df["pnl_pct"] <= 0]["pnl_yen"].sum()) or 1e-9
    return {
        "total_trades": len(df),
        "win_rate": round(len(wins) / len(df) * 100, 1),
        "avg_pnl_pct": round(df["pnl_pct"].mean(), 2),
        "total_pnl_yen": round(df["pnl_yen"].sum(), 0),
        "profit_factor": round(gross_p / gross_l, 2),
        "exit_counts": df["exit_reason"].apply(_normalize_exit_reason).value_counts().to_dict()
    }

def _format_report_plain(summary: dict) -> str:
    if summary["total_trades"] == 0: return "📊 シグナルなし"
    res = f"📊 **バックテスト結果**\n総数: {summary['total_trades']}回 / 勝率: {summary['win_rate']}%\n"
    res += f"平均損益: {summary['avg_pnl_pct']}% / 合計損益: {summary['total_pnl_yen']:,}円\n"
    res += f"PF: {summary['profit_factor']}\n\n【手じまい理由】\n"
    for r, c in summary["exit_counts"].items(): res += f"- {r}: {c}回\n"
    return res

def _format_report_with_gemini(summary: dict, top_trades: list[dict]) -> str:
    plain = _format_report_plain(summary)
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key: return plain
    try:
        client = genai.Client(api_key=api_key)
        resp = client.models.generate_content(model="gemini-2.0-flash", contents=f"結果を300字以内で分析して:\n{summary}\n上位:{top_trades[:3]}")
        return f"{plain}\n💡 **分析**\n{resp.text}"
    except: return plain

def _send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if url: requests.post(url, json={"content": content[:1990]})

# -----------------------------------------------------------------------
# メイン実行
# -----------------------------------------------------------------------

def run_backtest_and_report():
    start_time = datetime.now()
    cfg = _load_config()
    bt_params = _load_bt_params()
    min_score = float(bt_params.get("min_score", 55.0))
    
    db = DBManager()
    df_all = db.load_analysis_data(days=365 * 3)
    if df_all.empty: return

    niy_df = df_all[df_all["ticker"] == "NIY=F"].sort_values("date").copy()
    niy_df["m_change"] = niy_df["price"].pct_change() * 100
    crash_dates = set(niy_df[niy_df["m_change"] <= bt_params["market_crash_limit"]]["date"])

    all_signals = []
    tickers = [t for t in df_all["ticker"].unique() if t != "NIY=F"]
    min_days = cfg.get("filter", {}).get("min_data_days", 80)
    stop_high_enabled = cfg.get("filter", {}).get("stop_high", {}).get("enabled", True)
    scoring_cfg = cfg.get("scoring_logic", {})
    exclude_ranges = bt_params.get("exclude_score_ranges", [])

    print(f"🚀 バックテスト開始 (対象: {len(tickers)}銘柄 / 期間: 3年)")

    for ticker in tickers:
        df_ticker = df_all[df_all["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        if len(df_ticker) < min_days: continue

        df_ticker = _calculate_indicators(df_ticker)
        hits = scan_signals(df_ticker)
        if not hits: continue

        for hit in hits:
            i = hit.get('index')
            if i is None:
                match_idx = df_ticker.index[df_ticker['date'] == hit['date']].tolist()
                if not match_idx: continue
                i = match_idx[0]

            if i < min_days or i >= len(df_ticker) - 1: continue

            entry_date = df_ticker.iloc[i + 1]["date"]
            if entry_date in crash_dates: continue

            if stop_high_enabled and i >= 1:
                if _is_stop_high_internal(float(df_ticker.iloc[i]["price"]), float(df_ticker.iloc[i-1]["price"])):
                    continue

            score = calculate_score(pd.Series(hit), scoring_cfg)
            if score < min_score: continue
            if any(r[0] <= score < r[1] for r in exclude_ranges): continue

            all_signals.append({
                "date": pd.to_datetime(entry_date),
                "ticker": ticker,
                "score": score,
                "signal_type": hit.get("signal_type", "不明"),
                "df_ticker": df_ticker,
                "entry_idx": i + 1,
            })

    if not all_signals:
        print("シグナルなし")
        return

    sig_df = pd.DataFrame(all_signals)
    selected = sig_df.sort_values(["date", "score"], ascending=[True, False]).groupby("date").head(bt_params["max_daily_entries"])

    final_trades, free_dates = [], {}
    for _, sig in selected.iterrows():
        t = sig["ticker"]
        if t in free_dates and sig["date"] < pd.to_datetime(free_dates[t]): continue
        trade = _execute_trade(sig, bt_params, cfg)
        if trade:
            final_trades.append(trade)
            free_dates[t] = trade["exit_date"]

    summary = _calc_summary(final_trades, bt_params)
    report = _format_report_with_gemini(summary, sorted(final_trades, key=lambda x: x["pnl_pct"], reverse=True))
    _send_discord(report)
    print(f"✅ 完了 (所要時間: {datetime.now() - start_time})")

if __name__ == "__main__":
    run_backtest_and_report()
