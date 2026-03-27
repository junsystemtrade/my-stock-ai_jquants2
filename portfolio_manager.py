import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 2026年V2仕様：x-api-keyヘッダーに直接セット
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480" # ビックカメラ
        print(f"🚀 J-Quants V2 接続開始 (Key: {api_key[:5]}...)")
        
        # V2エンドポイント
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        
        # 認証が正しければ、ここで200 OKとJSONが返ります
        res = requests.get(price_url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # V2のカラム名をDB用に変換
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✨ {target_code} のデータをSupabaseへ格納しました！")
            else:
                print("⚠️ データが空です（市場休業日など）")
        else:
            print(f"❌ 認証エラー({res.status_code}): キーが反映されていないか、プラン制限の可能性があります。")
            print(f"詳細: {res.text[:100]}")

    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    sync_data()
