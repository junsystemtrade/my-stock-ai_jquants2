import os
import pandas as pd
import google.generativeai as genai
import requests
from database_manager import DBManager

def send_to_discord(content):
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url: return
    # 2000文字制限対策の分割送信
    for i in range(0, len(content), 1900):
        requests.post(url, json={"content": content[i:i+1900]})

def run_backtest_and_report():
    db = DBManager()
    all_data = db.load_analysis_data(days=1825) # 5年分
    trades = []
    for ticker, df in all_data.items():
        if len(df) < 85: continue
        df = df.copy()
        df['ma25'] = df['close'].rolling(25).mean()
        df['ma75'] = df['close'].rolling(75).mean()
        for i in range(75, len(df) - 7):
            curr = df.iloc[i]
            if curr['ma25'] > curr['ma75'] and curr['close'] > curr['ma25']:
                profit = (df.iloc[i+7]['close'] / df.iloc[i+1]['open']) - 1
                trades.append({"year": df.index[i].year, "profit": profit})
    
    tdf = pd.DataFrame(trades)
    if tdf.empty: return
    
    api_key = os.getenv("GOOGLE_API_KEY")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    
    summary = tdf.groupby('year')['profit'].agg(['count', 'mean']).to_string()
    prompt = f"5年間の株シストレ(25/75日MAゴールデンクロス)の結果を分析してください。\n通算勝率:{(tdf['profit']>0).mean():.1%}\n年別成績:\n{summary}"
    
    report = f"📉 **【5カ年長期バックテスト報告】**\n{model.generate_content(prompt).text}"
    send_to_discord(report)
