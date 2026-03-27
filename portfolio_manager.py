import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480" # ビックカメラ
        # Freeプランでも確実に取れる「3日前〜昨日」の範囲を指定
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        print(f"🚀 J-Quants V2 (Free) 取得開始: {target_code} ({start_date} ~ {end_date})")
        
        # Freeプランの制約を回避するため、from/to パラメータを付与
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}&from={start_date}&to={end_date}"
        
        # allow_redirects=False でログイン画面への逃げを封じる
        res = requests.get(price_url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✨ 保存完了！{len(df)}件の過去データを格納しました。")
            else:
                print(f"⚠️ 期間内({start_date}〜)にデータがありません。")
        elif res.status_code in [301, 302]:
            print(f"❌ 認証拒否: 最新データへのアクセス権限がないためリダイレクトされました。")
        else:
            print(f"❌ APIエラー {res.status_code}: {res.text[:100]}")

    except Exception as e:
        print(f"❌ 同期エラー詳細: {e}")
