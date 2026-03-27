import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        # 銘柄リスト取得
        print("🔍 J-Quants V2 銘柄リスト取得中...")
        list_url = "https://jpx-jquants.com/api/v2/listed/info"
        res_list = requests.get(list_url, headers=headers, timeout=20)
        
        # 銘柄 30480 (ビックカメラ) の取得
        target_code = "30480"
        print(f"🔄 同期開始: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers)
        
        if res_price.status_code == 200:
            data = res_price.json() # ここでエラーが出る場合、中身をプリント
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"})
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ {target_code} 保存完了")
        else:
            print(f"❌ APIエラー: {res_price.status_code} - {res_price.text}")

    except Exception as e:
        print(f"❌ 同期処理中に例外発生: {e}")
