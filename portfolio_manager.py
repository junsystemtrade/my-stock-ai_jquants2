import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    
    headers = {"x-api-key": api_key, "accept": "application/json"}
    
    # ❗Freeプランの鉄則: 12週間(84日)以上前の日付を指定
    # 2026年3月末から見て、余裕を持って「2025年12月1日」を狙います
    target_code = "3048"
    safe_date = "2025-12-01" 
    
    print(f"🎯 Freeプラン境界突破テスト: {target_code} (Date: {safe_date})")
    
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}&from={safe_date}&to={safe_date}"
    
    try:
        res = requests.get(url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200 and not res.text.startswith("/ja"):
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # DB保存処理（カラム名は前回同様）
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code
                df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"price","Volume":"volume"})
                db.save_prices(df[['ticker','date','open','high','low','price','volume']])
                print(f"✨ 12週間の壁を突破！{safe_date} のデータを取得・保存しました。")
            else:
                print("⚠️ 認証は通りましたが、データが空です。")
        else:
            print(f"❌ 依然としてリダイレクト。ステータス: {res.status_code}")
            print(f"この日付({safe_date})でもダメな場合、APIキー自体の権限を再確認する必要があります。")

    except Exception as e:
        print(f"❌ エラー: {e}")
