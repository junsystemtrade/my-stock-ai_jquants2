import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 認証ヘッダーを強化（x-api-key と Authorization の両方を試す形）
    headers = {
        "x-api-key": api_key,
        "Authorization": f"Bearer {api_key}",
        "accept": "application/json"
    }
    
    try:
        target_code = "30480" 
        print(f"🔄 J-Quants V2 データ取得開始: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        # リダイレクトを禁止して、エラー時に内容を確認できるようにする
        res = requests.get(price_url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={"Open":"open", "High":"high", "Low":"low", "Close":"price", "Volume":"volume"})
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
            else:
                print("⚠️ 取得データが空です。")
        else:
            print(f"❌ APIエラー: {res.status_code} / 応答: {res.text[:100]}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")
