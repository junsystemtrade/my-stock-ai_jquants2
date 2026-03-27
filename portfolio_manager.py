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
        # ❗最重要修正: 30480 ではなく 3048 (4桁) にします
        target_code = "3048" 
        
        # Freeプランでも確実に権限がある「2週間前」の1日分だけをテスト
        test_date = "2026-03-10"
        
        print(f"🎯 J-Quants V2 ターゲット修正: {target_code} (Date: {test_date})")
        
        # V2エンドポイント
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}&from={test_date}&to={test_date}"
        
        res = requests.get(price_url, headers=headers, timeout=20)
        
        if res.status_code == 200:
            if res.text.startswith("/ja"):
                print("❌ まだリダイレクトが発生しています。APIキーの有効化に時間がかかっている可能性があります。")
                return

            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code
                df = df.rename(columns={"Open":"open", "High":"high", "Low":"low", "Close":"price", "Volume":"volume"})
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✨ ついに成功！{target_code} のデータをDBに格納しました。")
            else:
                print(f"⚠️ 認証は成功しましたが、{test_date} のデータが空です。")
        else:
            print(f"❌ APIエラー {res.status_code}: {res.text[:100]}")

    except Exception as e:
        print(f"❌ システムエラー: {e}")
