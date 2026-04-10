"""
main.py
=======
クォータ対策 (429 RESOURCE_EXHAUSTED) を強化した最新安定版。
google-genai SDK と gemini-1.5-flash を使用。
"""

import os
import sys
import time
import requests
import pandas as pd
import yfinance as yf
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

def get_detailed_research(ticker: str, signal_type: str, reason: str) -> tuple:
    """
    リトライと待機、柔軟なパース機能を備えた企業リサーチ。
    """
    code = ticker.replace(".T", "")
    # 初期値（エラー時に表示される内容）
    name, summary, topic = code, "（リサーチ制限中）", "⚠️ Gemini APIのクォータ制限により、情報を取得できませんでした。"

    if not client:
        return name, summary, "（APIキー未設定）"

    # プロンプトを極限までシンプルにし、トークンと解析エラーを減らす
    prompt = f"銘柄コード{code}の日本企業について回答。1行目:企業名のみ、2行目:【事業概要】(30字以内)、3行目:【最新トピック】(100字以内)。投資助言禁止。日本語必須。"

    # 最大3回リトライ
    for attempt in range(3):
        try:
            # 実行前の待機（回数を重ねるごとに長くする）
            time.sleep(attempt * 5)
            
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=prompt,
            )
            
            text = response.text.strip()
            # 空行を除去してリスト化
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            if len(lines) >= 1:
                # 1行目: 企業名から装飾や番号を除去
                name = lines[0].split(':', 1)[-1] if ':' in lines[0] else lines[0]
                name = name.replace("1.", "").replace("**", "").strip()
                
                # 2行目以降から概要とトピックを抽出
                body = "\n".join(lines[1:])
                if "【最新トピック】" in body:
                    parts = body.split("【最新トピック】")
                    summary = parts[0].replace("【事業概要】", "").replace("2.", "").replace(":", "").strip()
                    topic = parts[1].replace("3.", "").replace(":", "").strip()
                else:
                    # 形式が崩れた場合の最低限の保護
                    summary = "（解析エラー：形式不一致）"
                    topic = body[:150]
                
                return name, summary, topic # 成功

        except Exception as e:
            print(f"⚠️ 銘柄{code} 試行{attempt+1}失敗: {e}")
            if "429" in str(e):
                # 429エラー時は冷却期間を設ける
                time.sleep(20)
            continue
            
    return name, summary, topic

def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ Discord Webhook URL 未設定")
        return
    # Discordの2000文字制限対策
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        try:
            requests.post(url, json={"content": chunk})
        except Exception as e:
            print(f"❌ Discord送信失敗: {e}")

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
    
    # STEP 4: シグナルスキャン
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

    # STEP 5: レポート作成
    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 20 + "\n"

    for i, s in enumerate(signals, 1):
        # 429エラー防止：1銘柄目の前にも、銘柄間にもしっかり待機を入れる
        print(f"⏳ 銘柄 {s['ticker']} リサーチ準備中...")
        time.sleep(15) 
            
        name, business, topic = get_detailed_research(s['ticker'], s['signal_type'], s['reason'])

        report += (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"**{name}** ({s['ticker']} / {int(s['price']):,}円)\n"
            f"**事業概要**: {business}\n"
            f"**シグナル**: {s['signal_type']}\n"
            f"**根拠**: {s['reason']}\n"
            f"**直近トピック**: {topic}\n"
            f"────────────────────\n"
        )

    send_discord(report)
    print("✅ 全プロセス正常終了")

if __name__ == "__main__":
    main()
