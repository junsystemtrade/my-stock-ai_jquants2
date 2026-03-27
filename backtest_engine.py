import os
import pandas as pd
import google.generativeai as genai
import database_manager
import requests

def run_backtest_and_report():
    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    db = database_manager.DBManager()
    # 直近30日のデータを取得
    df = db.load_analysis_data(days=30)
    
    if df.empty:
        print("⚠️ データ不足のためレポートをスキップします。")
        return

    prompt = f"""
    以下の株価データを分析し、福岡の投資家・村田さんへ
    Discord向けの投資レポート（日本語）を作成してください。
    
    データ概要:
    {df.head(50).to_string(index=False)}
    
    形式:
    🏛️ **【AI投資顧問：市場分析】**
    - 今日の概況
    - 注目銘柄のテクニカル分析
    - 明日の運用戦略
    """

    try:
        response = model.generate_content(prompt)
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if webhook_url:
            requests.post(webhook_url, json={"content": response.text})
            print("🚀 レポートを送信しました。")
    except Exception as e:
        print(f"❌ 分析失敗: {e}")

if __name__ == "__main__":
    run_backtest_and_report()
