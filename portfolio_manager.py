import os
import requests
import yfinance as yf
import pandas as pd
import database_manager

def get_id_token():
    """リフレッシュトークンから一時的なIDトークンを発行する"""
    refresh_token = os.getenv("JQUANTS_API_KEY")
    print("🔑 J-Quants 認証中...")
    # V2の認証エンドポイント
    auth_url = f"https://jpx-jquants.com/api/v2/token/auth_refresh?refreshtoken={refresh_token}"
    res = requests.post(auth_url)
    res.raise_for_status()
    return res.json().get("idToken")

def sync_data():
    db = database_manager.DBManager()
    
    try:
        # 1. IDトークン取得
        id_token = get_id_token()
        headers = {"Authorization": f"Bearer {id_token}"}
        
        # 2. 銘柄リスト取得
        print("🔍 上場銘柄リスト取得中...")
        list_url = "https://jpx-jquants.com/api/v2/listed/info"
        res_list = requests.get(list_url, headers=headers)
        res_list.raise_for_status()
        
        # 3. テスト実行（まずはビックカメラ: 30480）
        # 全銘柄一括取得も可能ですが、まずは確実に1件保存できるか確認
        target_code = "30480" 
        print(f"🔄 同期開始: {target_code}")
        
        # J-Quants V2 価格取得
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers)
        
        if res_price.status_code == 200:
            quotes = res_price.json().get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"})
                
                # DB保存
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ {target_code} 保存完了")
                
    except Exception as e:
        print(f"❌ 同期エラー: {e}")

if __name__ == "__main__":
    sync_data()
