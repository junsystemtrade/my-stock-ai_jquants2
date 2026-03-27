import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # セッションを作成し、リダイレクトをオフにして「APIとして」振る舞わせる
    session = requests.Session()
    session.headers.update({
        "x-api-key": api_key,
        "accept": "application/json",
        "User-Agent": "python-requests/2.31.0"
    })
    
    try:
        target_code = "30480" # ビックカメラ
        print(f"🔄 J-Quants V2 (Direct Key) 取得開始: {target_code}")
        
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        
        # allow_redirects=False で勝手にログイン画面(/ja)に行かせない
        res = session.get(price_url, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # V2のカラム名（Date, Open, High, Low, Close, Volume）を処理
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
            else:
                print("⚠️ 取得データが空です（市場休業日の可能性があります）")
        elif res.status_code in [301, 302]:
            print(f"❌ 認証失敗: ログイン画面へリダイレクトされました。APIキーが間違っている可能性があります。")
        else:
            print(f"❌ APIエラー: {res.status_code} / 内容: {res.text[:100]}")

    except Exception as e:
        print(f"❌ 同期エラー詳細: {e}")

if __name__ == "__main__":
    sync_data()
