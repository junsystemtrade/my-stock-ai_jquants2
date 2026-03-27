import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 2026年1月時点のV2 APIキー認証（Qiita記事準拠）
    # もしエラーが出る場合は Authorization: Bearer {api_key} も試せるよう headers を構成
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480" # ビックカメラ
        print(f"🔄 J-Quants V2 データ取得開始: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res = requests.get(price_url, headers=headers, timeout=20)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # V2のカラム名をDB形式に変換
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                # 保存実行
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
            else:
                print("⚠️ 取得データが空です。")
        else:
            print(f"❌ API認証失敗: {res.status_code} / {res.text[:100]}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")

if __name__ == "__main__":
    sync_data()
