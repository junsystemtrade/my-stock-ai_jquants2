import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 【重要】J-Quants V2のAPIキー直接認証は 'x-api-key' ヘッダーを使用します
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480"
        print(f"🔄 J-Quants V2 (x-api-key) で取得中: {target_code}")
        
        # 銘柄情報のURL (V2)
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers, timeout=20)
        
        if res_price.status_code == 200:
            data = res_price.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"
                })
                # DB保存
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ {target_code} の保存に成功しました！")
            else:
                print("⚠️ 応答データが空です（市場休業日など）")
        else:
            # 401や403ならAPIキーの設定ミス、302ならリダイレクト（ログイン画面へ）
            print(f"❌ APIエラー: {res_price.status_code}")
            print(f"デバッグ応答: {res_price.text[:100]}")

    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    sync_data()
