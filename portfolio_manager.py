import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 認証ヘッダー (V2 APIキー用)
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        # 30480 (ビックカメラ) を取得
        target_code = "30480"
        print(f"🔄 J-Quants V2 からデータ取得中: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers)
        
        # 応答が空でないか、JSONかどうかを厳密にチェック
        if res_price.status_code == 200 and res_price.text.strip():
            try:
                data = res_price.json()
                quotes = data.get("daily_quotes", [])
                if quotes:
                    df = pd.DataFrame(quotes)
                    df['date'] = pd.to_datetime(df['Date']).dt.date
                    df['ticker'] = target_code[:4]
                    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"})
                    db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                else:
                    print("⚠️ 取得データが空です。")
            except Exception as je:
                print(f"❌ JSON解析失敗: {je} / Response: {res_price.text[:100]}")
        else:
            print(f"❌ APIエラー: {res_price.status_code} / 内容: {res_price.text}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")
