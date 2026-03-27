import os
import pandas as pd
import google.generativeai as genai
import database_manager
import requests

def run_backtest_and_report():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ GOOGLE_API_KEY がありません")
        return
        
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')

    db = database_manager.DBManager()
    df = db.load_analysis_data(days=30)

    if df.empty:
        print("✅ 分析対象データが空です")
        return

    prompt = f"""
    以下の株価データを分析し、投資顧問として村田さんへアドバイスを生成してください。
    データ:
    {df.to_string(index=False)}
    
    出力:
    🏛️ **【AI投資顧問：市場分析レポート】**
    1. トレンド分析
    2. リスクとチャンス
    3. 明日の戦略
    """

    print("🧠 Gemini 2.0 分析中...")
    response = model.generate_content(prompt)
    report = response.text

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if webhook_url:
        requests.post(webhook_url, json={"content": report})
        print("🚀 Discordへ送信完了")
