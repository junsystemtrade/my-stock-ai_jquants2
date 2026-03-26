import os
import pandas as pd
import google.generativeai as genai
import requests
from database_manager import DBManager

def send_to_discord(content):
    """DiscordのWebhookを使用してメッセージを送信（文字数制限対策済み）"""
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url: return
    
    # Discordの1投稿2000文字制限を考慮して分割送信
    for i in range(0, len(content), 1900):
        requests.post(url, json={"content": content[i:i+1900]})

def run_backtest_and_report():
    """過去5年間のデータに基づきシミュレーションを行い、AIがレポートを作成する"""
    db = DBManager()
    all_data = db.load_analysis_data(days=1825) # 5年分ロード
    trades = []
    
    for ticker, df in all_data.items():
        if len(df) < 85: continue
        df = df.copy()
        df['ma25'] = df['close'].rolling(25).mean()
        df['ma75'] = df['close'].rolling(75).mean()
        
        # 過去の全日程をスキャン
        for i in range(75, len(df) - 7):
            curr = df.iloc[i]
            # シグナル発生：25日線 > 75日線 かつ 終値 > 25日線
            if curr['ma25'] > curr['ma75'] and curr['close'] > curr['ma25']:
                # 翌日始値で購入し、7営業日後の終値で売却したと仮定
                profit = (df.iloc[i+7]['close'] / df.iloc[i+1]['open']) - 1
                trades.append({"year": df.index[i].year, "profit": profit})
    
    tdf = pd.DataFrame(trades)
    if tdf.empty:
        send_to_discord("⚠️ バックテスト対象の取引データが不足しています。")
        return
    
    # Geminiによる結果の総括
    api_key = os.getenv("GOOGLE_API_KEY")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    
    # 年別の統計（取引数と平均利益率）
    summary = tdf.groupby('year')['profit'].agg(['count', 'mean']).to_string()
    prompt = (
        "以下の日本株シミュレーション結果（25/75日MA順張り戦略）を分析し、"
        "今後の改善案を含めた投資レポートを日本語で作成してください。\n\n"
        f"通算勝率: {(tdf['profit']>0).mean():.1%}\n"
        f"年別パフォーマンス:\n{summary}"
    )
    
    try:
        ai_report = model.generate_content(prompt).text
        final_msg = f"📉 **【5カ年長期バックテスト報告】**\n{ai_report}"
        send_to_discord(final_msg)
    except Exception as e:
        send_to_discord(f"⚠️ バックテストAI分析失敗: {e}")
