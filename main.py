"""
main.py
=======
クォータ対策を強化し、モデルを 1.5-flash に変更して安定化させたバージョン。
"""

import os
import sys
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

def get_detailed_research(ticker: str, signal_type: str, reason: str) -> tuple:
    """
    リトライ機能を備えた企業リサーチ。
    """
    code = ticker.replace(".T", "")
    name, summary, topic = code, "（調査中）", "（取得失敗）"

    if not client:
        return name, summary, "（APIキー未設定）"

    prompt = f"""
日本の銘柄「{code}」を日本語でリサーチし、以下の3行で出力せよ。
1行目: 企業名
2行目: 【事業概要】主力事業と特徴（30字以内）
3行目: 【最新トピック】直近のニュースや注目点（100字以内）
※投資助言禁止。日本語必須。
"""

    # 最大3回までリトライ
    for attempt in range(3):
        try:
            # 安定性の高い 1.5-flash を使用
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=prompt,
            )
            res_text = [line.strip() for line in response.text.strip().split('\n') if line.strip()]
            
            if len(res_text) >= 1:
                name = res_text[0].replace("**", "")
                
                # 残りのテキストから概要とトピックを抽出
                body = "\n".join(res_text[1:])
                if "【最新トピック】" in body:
                    parts = body.split("【最新トピック】")
                    summary = parts[0].replace("【事業概要】", "").replace(":", "").strip()
                    topic = parts[1].replace(":", "").strip()
                else:
                    topic = body[:150]
                
                return name, summary, topic # 成功したら抜ける

        except Exception as e:
            print(f"⚠️ リサーチ試行 {attempt+1} 回目失敗: {e}")
            if "429" in str(e):
                time.sleep(10) # 429エラー時は長めに待機
            else:
                time.sleep(2)
    
    return name, "（制限により取得不可）", "⚠️ Gemini APIが混雑しています。時間を空けて実行してください。"

def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url: return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        requests.post(url, json={"content": chunk})

def main():
    print("🚀 システム起動: 安定リサーチモード")
    cfg = _load_config()

    portfolio_manager.sync_data()
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)
    if daily_data.empty: return

    market_change, market_status = _get_market_condition()
    raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
    if not raw_signals:
        send_discord(f"📊 地合い: {market_status}\n✅ 対象なし")
        return

    # スコアリング
    scoring_cfg = cfg.get('scoring_logic', {})
    for s in raw_signals:
        s["score"] = calculate_score(pd.Series(s), scoring_cfg)
    signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 20 + "\n"

    for i, s in enumerate(signals, 1):
        if i > 1:
            time.sleep(5) # 銘柄間も長めに待機
            
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
    print("✅ 送信完了")

if __name__ == "__main__":
    main()
