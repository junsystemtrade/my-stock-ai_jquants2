import os
import pandas as pd
import google.generativeai as genai
import database_manager
import requests

def run_backtest_and_report():
    # Gemini セットアップ
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ GOOGLE_API_KEY が設定されていません")
        return
        
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')

    # データ読み込み
    db = database_manager.DBManager()
    df = db.load_analysis_data(days=30) # 直近30日の動きを分析

    if df.empty:
        print("✅ 分析対象データがありません")
        return

    # AIへのプロンプト作成
    prompt = f"""
    以下の株価データ（直近30日）を分析し、投資家へのアドバイスを生成してください。
    データ概要:
    {df.to_string(index=False)}
    
    出力フォーマット:
    🏛️ **【AI投資顧問：市場分析レポート】**
    1. 注目銘柄のトレンド分析
    2. リスクとチャンス
    3. 明日の戦略
    """

    print("🧠 Gemini 2.0 が思考中...")
    response = model.generate_content(prompt)
    report_content = response.text

    # Discord通知
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if webhook_url:
        requests.post(webhook_url, json={"content": report_content})
        print("🚀 Discordへレポートを送信しました")
    else:
        print(f"📄 生成レポート:\n{report_content}")

if __name__ == "__main__":
    run_backtest_and_report()
