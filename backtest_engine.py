import os
import pandas as pd
from google import genai
import database_manager
import requests

def run_backtest_and_report():
    # 1. クライアント作成
    client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
    
    # 2. DBからデータをロード
    db = database_manager.DBManager()
    df = db.load_analysis_data(days=30)
    
    if df.empty:
        print("⚠️ データ不足のためレポートをスキップします。")
        return

    # 3. プロンプト作成（村田さんの好みに合わせて少しトーンを調整）
    prompt = f"""
    以下の株価データを元に、福岡の投資家・村田さんへ
    Discord向けの投資レポートを日本語で作成してください。
    
    データ概要:
    {df.to_string(index=False)}
    
    構成:
    🏛️ **【AI投資顧問：市場分析】**
    - 本日の注目銘柄（3048 ビックカメラ等）の動き
    - RSIや移動平均等の視点からの示唆
    - 福岡の夜に贈る、明日の運用戦略アドバイス
    
    ※2000文字以内で、簡潔かつ情熱的に回答してください。
    """

    try:
        # 4. Gemini 2.0 Flash で生成
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        
        report_text = response.text

        # 5. Discord送信（文字数制限対策）
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if webhook_url and report_text:
            # 万が一2000文字を超えたらカット
            payload = {"content": report_text[:1990]} 
            res = requests.post(webhook_url, json=payload)
            
            if res.status_code == 204 or res.status_code == 200:
                print("🚀 Discordへレポートを送信しました！")
            else:
                print(f"❌ Discord送信エラー: {res.status_code}")
                
    except Exception as e:
        print(f"❌ 分析失敗: {e}")

if __name__ == "__main__":
    run_backtest_and_report()
