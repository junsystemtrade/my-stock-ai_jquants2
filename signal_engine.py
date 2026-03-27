import os
import pandas as pd
from google import genai

def scan_signals(df):
    """テクニカル指標を計算し、AIで銘柄を分析"""
    if df.empty:
        return []

    # テクニカル指標（RSI, EMA）の簡易計算
    df['ema_short'] = df.groupby('ticker')['close'].transform(lambda x: x.ewm(span=12).mean())
    df['ema_long'] = df.groupby('ticker')['close'].transform(lambda x: x.ewm(span=26).mean())
    
    # 直近のシグナル銘柄を抽出（ゴールデンクロス等）
    latest = df.groupby('ticker').tail(1).copy()
    targets = latest[latest['ema_short'] > latest['ema_long']].head(5) # 上位5件

    if targets.empty:
        return []

    # Gemini API でインサイト生成
    client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY"))
    
    results = []
    for _, row in targets.iterrows():
        prompt = f"銘柄 {row['ticker']} が現在の価格 {row['close']}円でゴールデンクロスしました。短期的な展望を30文字以内で分析して。"
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash", # 最新モデル
                contents=prompt
            )
            results.append({
                "ticker": row['ticker'],
                "price": row['close'],
                "insight": response.text
            })
        except:
            results.append({
                "ticker": row['ticker'],
                "price": row['close'],
                "insight": "AI分析スキップ：テクニカル指標は良好です。"
            })
            
    return results
