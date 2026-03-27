import os
import pandas as pd
import google.generativeai as genai
import database_manager
import requests

def run_backtest_and_report():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ GOOGLE_API_KEY が未設定です。")
        return
        
    # Gemini 2.0 セットアップ
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')

    db = database_manager.DBManager()
    df = db.load_analysis_data(days=14) # 直近2週間分を分析

    if df.empty:
        print("⚠️ 分析対象データがDBにありません。")
        return

    prompt = f"""
    以下の最新株価データを分析し、投資家・村田さんへのアドバイスを作成してください。
    
    データ概要:
    {df.to_string(index=False)}
    
    出力形式:
    🏛️ **【AI投資顧問：市場分析レポート】**
    1. 保有・注目銘柄の動向
    2. テクニカル/ファンダメンタル視点のリスク
    3. 明日以降の戦略案
    """

    print("🧠 Gemini 2.0 が分析レポートを生成中...")
    try:
        response = model.generate_content(prompt)
        report_text = response.text

        # Discord通知
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if webhook_url:
            requests.post(webhook_url, json={"content": report_text})
            print("🚀 Discordへのレポート送信が完了しました。")
        else:
            print(f"📄 生成されたレポート:\n{report_text}")
    except Exception as e:
        print(f"❌ レポート生成/送信失敗: {e}")

if __name__ == "__main__":
    run_backtest_and_report()
