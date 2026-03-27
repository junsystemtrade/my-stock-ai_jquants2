import os
import pandas as pd
from google import genai
import database_manager
import requests

def run_backtest_and_report():
    # 最新SDKのクライアント作成
    client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
    
    db = database_manager.DBManager()
    df = db.load_analysis_data(days=30)
    
    if df.empty:
        print("⚠️ データ不足のためレポートをスキップします。")
        return

    # 村田さんの注目銘柄（3048など）を中心に分析
    prompt = f"""
    以下の株価データ（直近30日）を元に、福岡の投資家・村田さんへ
    Discord向けの投資レポートを日本語で作成してください。
    
    データ概要:
    {df.tail(20).to_string(index=False)}
    
    構成:
    🏛️ **【AI投資顧問：市場分析】**
    - 本日の注目銘柄（3048 ビックカメラ等）の動き
    - テクニカル指標（RSI/EMA等）からの示唆
    - 明日の運用戦略アドバイス
    """

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if webhook_url:
            requests.post(webhook_url, json={"content": response.text})
            print("🚀 Discordへレポートを送信しました！")
    except Exception as e:
        print(f"❌ 分析失敗: {e}")

if __name__ == "__main__":
    run_backtest_and_report()
