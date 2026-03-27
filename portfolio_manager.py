import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # J-Quants V2 API: APIキーを直接使う場合の標準ヘッダー
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480"
        print(f"🔄 J-Quants V2 取得開始: {target_code}")
        
        # allow_redirects=False にして、勝手にログイン画面に飛ばないように監視
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
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
                print("⚠️ データが空です。")
        elif res.status_code in [301, 302]:
            print(f"❌ 認証失敗: ログイン画面({res.headers.get('Location')})へリダイレクトされました。APIキーを確認してください。")
        else:
            print(f"❌ APIエラー: {res.status_code} / {res.text}")

    except Exception as e:
        print(f"❌ 同期エラー詳細: {e}")
