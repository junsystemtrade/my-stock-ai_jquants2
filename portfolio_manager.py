import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    # GitHub Secrets の APIキーを取得
    api_key = os.getenv("JQUANTS_API_KEY")
    
    if not api_key:
        print("❌ JQUANTS_API_KEY が設定されていません。")
        return

    # V2の認証ヘッダー
    headers = {
        "Authorization": f"Bearer {api_key}",
        "accept": "application/json"
    }
    
    try:
        # 1. 銘柄リスト取得テスト
        print("🔍 J-Quants V2 接続テスト開始...")
        list_url = "https://jpx-jquants.com/api/v2/listed/info"
        res_list = requests.get(list_url, headers=headers, timeout=20)
        
        if res_list.status_code != 200:
            print(f"❌ 認証エラー: ステータス {res_list.status_code}")
            print(f"メッセージ: {res_list.text}")
            return

        # 2. ビックカメラ(30480)を取得
        target_code = "30480"
        print(f"🔄 データ取得中: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res_price = requests.get(price_url, headers=headers)
        
        # JSONデコードエラーを防ぐためのチェック
        if res_price.status_code == 200 and res_price.text.strip():
            data = res_price.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"})
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
            else:
                print("⚠️ データが空です。")
        else:
            print(f"❌ 価格取得エラー: {res_price.status_code} / {res_price.text}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")
