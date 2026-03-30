import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import database_manager

def sync_data():
    db = database_manager.DBManager()
    # キーを読み込み、前後の空白を完全に除去
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    
    if not api_key:
        print("❌ APIキーが設定されていません。")
        return

    print(f"🔑 使用中のAPIキー(先頭5文字): {api_key[:5]}")
    
    headers = {
        "x-api-key": api_key, 
        "accept": "application/json"
    }
    
    # ターゲット: 3048 (ビックカメラ) / 3月23日
    target_date = "2026-03-23"
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code=3048&from={target_date}&to={target_date}"
    
    try:
        # allow_redirects=False で "/ja" への逃げを封鎖
        res = requests.get(url, headers=headers, timeout=20, allow_redirects=False)
        
        print(f"📡 HTTPステータス: {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = "3048"
                df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"price","Volume":"volume"})
                db.save_prices(df[['ticker','date','open','high','low','price','volume']])
                print("✨ J-Quants V2 接続・保存に完全成功しました！")
            else:
                print("⚠️ 認証は通りましたが、データが空です。")
        else:
            print(f"❌ 認証失敗。ステータス: {res.status_code}")
            print(f"内容: {res.text[:100]}") # ログインHTMLが返っているか確認

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
