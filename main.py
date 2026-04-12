"""
main.py
=======
クォータ対策 (429 RESOURCE_EXHAUSTED) を強化した最新安定版。
google-genai SDK と gemini-2.0-flash を使用。
JPXマスターによる銘柄名取得・1銘柄1通知方式に対応。
"""

import os
import time
import requests
import pandas as pd
from google import genai

import portfolio_manager
import signal_engine
import backtest_engine
from database_manager import DBManager
from scoring_system import calculate_score
from signal_engine import _load_config, _get_market_condition

# --- Gemini API 設定 ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

# -----------------------------------------------------------------------
# JPXマスターから銘柄名取得（キャッシュ付き）
# -----------------------------------------------------------------------
_ticker_name_cache: dict = {}

def _load_ticker_names() -> dict:
    """JPXマスターから銘柄名辞書を取得してキャッシュする。"""
    global _ticker_name_cache
    if _ticker_name_cache:
        return _ticker_name_cache
    try:
        stock_map = portfolio_manager.get_target_tickers()
        _ticker_name_cache = {
            k.replace(".T", ""): v["name"]
            for k, v in stock_map.items()
        }
        print(f"✅ 銘柄名キャッシュ構築完了: {len(_ticker_name_cache)} 件")
    except Exception as e:
        print(f"⚠️ 銘柄名キャッシュ構築失敗: {e}")
    return _ticker_name_cache


def _get_company_name(code: str) -> str | None:
    """JPXマスターから銘柄名を取得する。"""
    names = _load_ticker_names()
    return names.get(code, None)


# -----------------------------------------------------------------------
# 企業リサーチ
# -----------------------------------------------------------------------
def get_detailed_research(ticker: str, signal_type: str, reason: str) -> tuple:
    """
    銘柄名はJPXマスターから取得。
    事業概要・トピック・シグナル考察はGeminiで取得。
    戻り値: (name, summary, topic, consideration)
    """
    code = ticker.replace(".T", "")
    name = code
    summary = "（リサーチ制限中）"
    topic = "⚠️ 情報を取得できませんでした。"
    consideration = "（取得できませんでした）"

    # STEP1: JPXマスターから銘柄名を取得
    jpx_name = _get_company_name(code)
    if jpx_name:
        name = jpx_name
        print(f"✅ 銘柄名取得成功: {code} → {name}")

    if not client:
        return name, summary, topic, consideration

    # STEP2: Geminiで詳細情報を取得（1銘柄1通知なので文字数を最大活用）
    prompt = f"""企業名:{jpx_name or code}（銘柄コード{code}）について以下の形式で回答してください。
【事業概要】（300字以内）
【直近トピック】（800字以内、決算・新製品・業績修正・株価動向など具体的な内容を含めること）
【シグナルとの関連】シグナル「{signal_type}」根拠「{reason}」との関連を500字以内で詳しく考察
投資助言は禁止。日本語必須。"""

    for attempt in range(3):
        try:
            time.sleep(attempt * 5)
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
            )
            text = response.text.strip()
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            body = "\n".join(lines)

            # 各セクションをパース
            if "【直近トピック】" in body and "【シグナルとの関連】" in body:
                parts1 = body.split("【直近トピック】")
                summary = parts1[0].replace("【事業概要】", "").replace(":", "").strip()
                parts2 = parts1[1].split("【シグナルとの関連】")
                topic = parts2[0].strip()
                consideration = parts2[1].strip()
            elif "【直近トピック】" in body:
                parts1 = body.split("【直近トピック】")
                summary = parts1[0].replace("【事業概要】", "").replace(":", "").strip()
                topic = parts1[1].strip()
            else:
                summary = body[:300]

            return name, summary, topic, consideration

        except Exception as e:
            print(f"⚠️ 銘柄{code} 試行{attempt+1}失敗: {e}")
            if "429" in str(e):
                time.sleep(20)
            continue

    return name, summary, topic, consideration


# -----------------------------------------------------------------------
# Discord通知
# -----------------------------------------------------------------------
def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ Discord Webhook URL 未設定")
        return
    # 万が一2000字を超えた場合の安全策
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        try:
            requests.post(url, json={"content": chunk})
        except Exception as e:
            print(f"❌ Discord送信失敗: {e}")


# -----------------------------------------------------------------------
# メイン処理
# -----------------------------------------------------------------------
def main():
    print("🚀 システム起動: 安定リサーチモード")
    cfg = _load_config()

    # STEP 1-2: データ同期とロード
    try:
        portfolio_manager.sync_data()
        db = DBManager()
        daily_data = db.load_analysis_data(days=150)
    except Exception as e:
        print(f"❌ 初期エラー: {e}")
        return

    if daily_data.empty:
        print("⚠️ データなし")
        return

    # STEP 3: 市場環境
    market_change, market_status = _get_market_condition()

    # STEP 4: 手じまいシグナルチェック（買いスキャンより先に実行）
    exit_signals = signal_engine.check_exit_signals(daily_data)
    if exit_signals:
        exit_report = "🚨 **【手じまいシグナル】**\n"
        exit_report += "━" * 20 + "\n"
        for e in exit_signals:
            pnl_str = f"{e['pnl_pct']:+.2f}%"
            pnl_emoji = "📈" if e['pnl_pct'] >= 0 else "📉"
            exit_report += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"**{e['ticker']}**\n"
                f"**理由**: {e['exit_reason']}\n"
                f"**買値**: {int(e['entry_price']):,}円 → **現値**: {int(e['current_price']):,}円\n"
                f"**損益**: {pnl_emoji} {pnl_str}\n"
                f"**買いシグナル日**: {e['entry_date']}\n"
                f"────────────────────\n"
            )
        send_discord(exit_report)

    # STEP 5: 買いシグナルスキャン
    raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
    if not raw_signals:
        send_discord(f"📊 本日の地合い: **{market_status}**\n✅ スキャン完了：条件を満たす銘柄はありませんでした。")
        return

    # スコアリング
    scoring_cfg = cfg.get('scoring_logic', {})
    for s in raw_signals:
        s["score"] = calculate_score(pd.Series(s), scoring_cfg)

    # 上位3件に絞り込み
    signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    # STEP 6: ヘッダーを先に送信
    _load_ticker_names()
    header = (
        f"🏛️ **【株式シグナル検知：厳選TOP3】**\n"
        f"📊 判定地合い: **{market_status}**\n"
        f"{'━' * 20}"
    )
    send_discord(header)

    # STEP 7: 1銘柄1通知でレポート送信
    for i, s in enumerate(signals, 1):
        print(f"⏳ 銘柄 {s['ticker']} リサーチ準備中...")
        time.sleep(15)

        name, business, topic, consideration = get_detailed_research(
            s['ticker'], s['signal_type'], s['reason']
        )

        # ★ 前日終値・前日比を計算
        df_ticker = daily_data[daily_data["ticker"] == s["ticker"]].sort_values("date")
        prev_close = None
        prev_change_str = "（取得不可）"
        if len(df_ticker) >= 2:
            latest_price = float(df_ticker.iloc[-1]["price"])
            prev_price   = float(df_ticker.iloc[-2]["price"])
            prev_close   = latest_price
            change_pct   = (latest_price - prev_price) / prev_price * 100
            change_yen   = latest_price - prev_price
            change_emoji = "📈" if change_pct >= 0 else "📉"
            prev_change_str = f"{change_emoji} {change_yen:+.0f}円 ({change_pct:+.2f}%)"

        report = (
            f"**{i}/3銘柄目**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"**{name}** ({s['ticker']})\n"
            f"**前日終値**: {int(s['price']):,}円　{prev_change_str}\n"
            f"**シグナル**: {s['signal_type']}\n"
            f"**根拠**: {s['reason']}\n"
            f"**スコア**: {s['score']:.1f}点\n"
            f"────────────────────\n"
            f"📋 **事業概要**\n{business}\n"
            f"────────────────────\n"
            f"📰 **直近トピック**\n{topic}\n"
            f"────────────────────\n"
            f"🔍 **シグナルとの関連**\n{consideration}\n"
        )
        send_discord(report)

    # STEP 8: 買いシグナル銘柄をDBに保存
    today = daily_data["date"].max()
    for s in signals:
        db.save_position(
            ticker=s["ticker"],
            entry_date=today,
            entry_price=s["price"],
            signal_type=s["signal_type"],
        )

    print("✅ 全プロセス正常終了")


if __name__ == "__main__":
    main()
