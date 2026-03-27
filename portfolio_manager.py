import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY") # V2 APIキー
    
    if not api_key:
        print("❌ JQUANTS_API_KEY が設定されていません。")
        return

    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        # 1. 銘柄リスト取得テスト
        print("🔍 J-Quants V2 銘柄リスト取得中...")
        list_url = "https://jpx-jquants.com/api/v2/listed/info"
        res_list = requests.get(list_url, headers=headers, timeout=20)
        
        if res_list.status_code != 200:
            print(f"❌ 認証またはリスト取得失敗: {res_list.status_code}")
            return

        # 2. ビックカメラ(30480)を取得
        target_code = "30480"
        print(f"🔄 データ取得開始: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers)
        
        if res_price.status_code == 200:
            quotes = res_price.json().get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"
                })
                # 保存実行
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
            else:
                print("⚠️ 取得データが空です。")
        else:
            print(f"❌ 価格取得エラー: {res_price.status_code}")

    except Exception as e:
        print(f"❌ エラー発生: {e}")

if __name__ == "__main__":
    sync_data()
