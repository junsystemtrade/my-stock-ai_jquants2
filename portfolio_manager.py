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
    
    # ターゲット: 3048 (ビックカメラ)
    # ❗Freeプランで確実に権限がある「1週間前」の特定日を狙い撃ちします
    target_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    code = "3048"
    
    print(f"🎯 J-Quants V2 執念の接続テスト: {code} (Target Date: {target_date})")
    
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={code}&from={target_date}&to={target_date}"
    
    try:
        # allow_redirects=False にして、リダイレクトされたら即エラーで落とします
        res = requests.get(url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            
            if quotes:
                df = pd.DataFrame(quotes)
                # V2のカラム名をDB用に整形
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = code
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                # Supabaseに保存（ここで DATABASE_URL の修正が効いてきます！）
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✨ J-Quants V2 からのデータ着弾に成功しました！")
            else:
                print(f"⚠️ 指定日({target_date})にデータが存在しません。市場休業日かもしれません。")
                
        elif res.status_code in [301, 302]:
            print(f"❌ 依然としてリダイレクト(302)されます。")
            print(f"原因: APIキーがFreeプランとして認識されていないか、日付の権限不足です。")
        else:
            print(f"❌ APIエラー {res.status_code}: {res.text}")

    except Exception as e:
        print(f"❌ 致命的なエラー: {e}")

if __name__ == "__main__":
    sync_data()
