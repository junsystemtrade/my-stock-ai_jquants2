import os
import requests
import yfinance as yf
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    
    # GitHub Secrets: JQUANTS_API_KEY (v2のAPIキー)
    api_key = os.getenv("JQUANTS_API_KEY")
    if not api_key:
        print("❌ JQUANTS_API_KEY が設定されていません。")
        return

    # V2 APIでは、このヘッダーだけで認証が完結します
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        # 1. 銘柄リスト取得 (V2)
        print("🔍 J-Quants V2 銘柄リスト取得中...")
        list_url = "https://jpx-jquants.com/api/v2/listed/info"
        res_list = requests.get(list_url, headers=headers, timeout=20)
        
        if res_list.status_code != 200:
            print(f"❌ リスト取得失敗: {res_list.status_code} {res_list.text}")
            return
            
        # 2. テストとして「ビックカメラ(30480)」を同期
        target_code = "30480"
        print(f"🔄 同期開始: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers)
        
        if res_price.status_code == 200:
            quotes = res_price.json().get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # V2のカラム名に合わせて処理
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                # DBのカラム名(小文字)へリネーム
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"
                })
                
                # DB保存実行
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ {target_code} のデータをDBへ保存しました。")
        else:
            print(f"⚠️ 価格データ取得失敗: {res_price.status_code}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")

if __name__ == "__main__":
    sync_data()
