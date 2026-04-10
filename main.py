"""
main.py
=======
毎日の銘柄スキャン + Discord 通知のエントリーポイント。
GitHub Actions の daily workflow から呼ばれる。
"""

import os
import sys
import requests
import pandas as pd
import yfinance as yf
# 新しい google-genai SDK をインポート
from google import genai

import portfolio_manager
import signal_engine
import backtest_engine
from database_manager import DBManager
from scoring_system import calculate_score
from signal_engine import _load_config, _get_market_condition

# --- Gemini API 設定 (google-genai 対応) ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if GOOGLE_API_KEY:
    # 新しい SDK のクライアント生成方法
    client = genai.Client(api_key=GOOGLE_API_KEY)
else:
    client = None

# -----------------------------------------------------------------------
# 外部情報の取得 (Gemini & yfinance)
# -----------------------------------------------------------------------
def get_company_info_and_topic(ticker: str, company_name: str):
    """事業概要と直近トピックを取得する"""
    business_summary = "（企業調査エラー）"
    topic_comment = "（トピック取得エラー）"
    
    # 1. yfinance で事業概要を取得
    try:
        t_info = yf.Ticker(ticker).info
        # 日本語の概要があれば優先、なければ英語の短縮名など
        business_summary = t_info.get('longBusinessSummary') or t_info.get('shortName') or "（情報なし）"
        # 文字数制限（Discord 対策で短くカット）
        if len(business_summary) > 60:
            business_summary = business_summary[:57] + "..."
    except Exception as e:
        print(f"⚠️ yfinance エラー ({ticker}): {e}")

    # 2. Gemini で直近トピックを生成 (google-genai SDK 仕様)
    if client:
        prompt = f"""
        日本の株式銘柄「{company_name} ({ticker})」について、ここ数日の重要ニュースやトピックを100文字程度で簡潔に要約してください。
        特にニュースがない場合は、その企業の現在の強みについて触れてください。
        """
        try:
            # 新しい SDK では client.models.generate_content を使用
            response = client.models.generate_content(
                model='gemini-1.5-flash',
                contents=prompt
            )
            topic_comment = response.text.strip()
        except Exception as e:
            topic_comment = f"（Geminiエラー: {e}）"
    else:
        topic_comment = "（GOOGLE_API_KEY 未設定）"

    return business_summary, topic_comment

# -----------------------------------------------------------------------
# Discord 通知
# -----------------------------------------------------------------------
def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ DISCORD_WEBHOOK_URL が未設定です。通知をスキップします。")
        return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        res = requests.post(url, json={"content": chunk})
        if res.status_code not in (200, 204):
            print(f"❌ Discord 送信エラー: {res.status_code}")

# -----------------------------------------------------------------------
# メイン処理 (変更なし)
# -----------------------------------------------------------------------
def main():
    print("🚀 システム起動: 株式分析プロセスを開始します")
    
    cfg = _load_config()

    # 1. データ同期
    print("\n--- STEP 1: データ同期 ---")
    try:
        portfolio_manager.sync_data()
    except Exception as e:
        msg = f"❌ データ同期エラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # 2. データロード
    print("\n--- STEP 2: データロード ---")
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)

    if daily_data.empty:
        msg = "⚠️ DBにデータがありません。先にバックフィルを実行してください。"
        print(msg)
        send_discord(msg)
        sys.exit(0)

    # 3. 市場環境チェック
    print("\n--- STEP 3: 市場環境チェック ---")
    market_change, market_status = _get_market_condition()
    
    if market_status == "不明":
        print("ℹ️ NIY=Fデータが不足しているため、地合い判定は「不明」として続行します。")

    crash_threshold = cfg.get("filter", {}).get("market_breaker", {}).get("drop_threshold_pct", -2.0)
    print(f"市場ステータス: {market_status} (NIY=F: {market_change:+.2f}%)")

    if market_change <= crash_threshold:
        msg = (
            f"📉 **【市場警戒：スキャン停止】**\n"
            f"日経平均先物が大幅下落（{market_change:+.2f}%）したため、リスク回避として新規探索を中止しました。\n"
        )
        print(msg)
        send_discord(msg)
        return
    
    print("✅ 市場環境良好。スキャンを継続します。")

    # 4. シグナルスキャン & スコアリング
    print("\n--- STEP 4: シグナルスキャン & スコアリング ---")
    try:
        raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
        
        if not raw_signals:
            send_discord("✅ 本日のスキャン完了：基準を満たす銘柄はありませんでした。")
            return

        scoring_cfg = cfg.get('scoring_logic', {})
        for s in raw_signals:
            s["score"] = calculate_score(pd.Series(s), scoring_cfg)
        
        signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    except Exception as e:
        msg = f"❌ 解析プロセスエラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # 5. Discord 通知
    print("\n--- STEP 5: Discord 通知 ---")
    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 20 + "\n"

    for i, s in enumerate(signals, 1):
        ticker = s['ticker']
        company_name = s.get('company_name') or ticker.replace('.T', '')
        
        # 追加情報の取得
        business, topic = get_company_info_and_topic(ticker, company_name)

        report += (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"**{company_name}** ({ticker} / {int(s['price']):,}円)\n"
            f"**事業概要**: {business}\n"
            f"**シグナル**: {s['signal_type']}\n"
            f"**根拠**: {s['reason']}\n"
            f"**直近トピック**: {topic}\n"
            f"────────────────────\n"
        )

    send_discord(report)
    print(f"✅ 上位 {len(signals)} 件を送信してプロセス正常終了")


if __name__ == "__main__":
    main()
