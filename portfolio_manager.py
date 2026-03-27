import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    if not api_key:
        print("❌ JQUANTS_API_KEY が設定されていません。")
        return

    # V2公式推奨：x-api-key ヘッダーを使用
    # AuthorizationヘッダーはV1の名残なので、V2ではこちらが優先されます
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480" # ビックカメラ
        print(f"🔄 J-Quants V2 (API Key方式) 取得開始: {target_code}")
        
        # V2エンドポイント
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        
        # リダイレクトを明示的に禁止 (ログイン画面へ飛ばさない)
        res = requests.get(price_url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # V2のカラム名（Date, Open, High, Low, Close, Volume）
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ {target_code} の保存に成功しました！")
            else:
                print("⚠️ 取得データが空です。")
        elif res.status_code in [301, 302]:
            # ここでログイン画面に飛ばそうとしているなら、APIキー自体がサーバーに拒否されています
            print(f"❌ 認証拒否: APIキーがV2として認識されていません。リダイレクト先: {res.headers.get('Location')}")
        else:
            print(f"❌ APIエラー: {res.status_code} / 応答内容: {res.text[:100]}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")
