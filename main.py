"""
main.py
=======
クォータ制限 (429) 対策として待機時間を導入。
日本語での企業リサーチを安定化させたバージョン。
"""

import os
import sys
import time  # 待機用
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
    クォータ制限を考慮しつつ、Gemini 2.0 Flash で日本語リサーチを行う。
    """
    code = ticker.replace(".T", "")
    name, summary, topic = code, "（調査中）", "（トピック取得エラー）"

    if not client:
        return name, summary, "（APIキー未設定）"

    prompt = f"""
あなたは日本の株式市場に精通したリサーチアナリストです。以下の銘柄を日本語でリサーチしてください。

銘柄コード: {code}
シグナル: {signal_type} ({reason})

【出力形式】
1行目: 企業名
2行目: 【事業概要】（30文字以内の日本語要約）
3行目: 【最新トピック】（100文字以内のニュースや背景）

※投資助言は行わず、事実のみを述べてください。日本語で出力してください。
"""

    try:
        # 1.5 Flash よりも 2.0 Flash の方が制限が厳しいため、エラー時は 1.5 へ切り替える工夫も可能ですが
        # まずは 2.0 Flash でリトライ間隔を空けて対応します
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        res_text = response.text.strip().split('\n')
        
        if len(res_text) >= 1:
            name = res_text[0].strip().replace("**", "") # 装飾除去
        
        full_body = "\n".join(res_text[1:])
        if "【最新トピック】" in full_body:
            parts = full_body.split("【最新トピック】")
            summary = parts[0].replace("【事業概要】", "").replace(":", "").strip()
            topic = parts[1].replace(":", "").strip()
        else:
            topic = full_body[:150] # 形式が崩れた場合のバックアップ

    except Exception as e:
        if "429" in str(e):
            topic = "⚠️ Gemini APIの無料枠制限に達しました。しばらく時間を置いて再試行してください。"
        else:
            topic = f"（リサーチエラー: {e}）"

    return name, summary, topic

def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url: return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        requests.post(url, json={"content": chunk})

def main():
    print("🚀 システム起動: 日本語リサーチ & クォータ対策モード")
    cfg = _load_config()

    portfolio_manager.sync_data()
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)
    if daily_data.empty: return

    market_change, market_status = _get_market_condition()
    raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
    if not raw_signals:
        send_discord(f"📊 地合い: {market_status}\n✅ 本日のスキャン完了：対象なし")
        return

    scoring_cfg = cfg.get('scoring_logic', {})
    for s in raw_signals:
        s["score"] = calculate_score(pd.Series(s), scoring_cfg)
    
    signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 20 + "\n"

    for i, s in enumerate(signals, 1):
        # 銘柄ごとのリクエストの間に 2秒 の待機時間を設ける（429対策）
        if i > 1:
            time.sleep(2)
            
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
    print("✅ レポート送信完了")

if __name__ == "__main__":
    main()
