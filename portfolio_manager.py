import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    # GitHub Secrets: JQUANTS_API_KEY (V2用のAPIキー)
    api_key = os.getenv("JQUANTS_API_KEY")
    
    if not api_key:
        print("❌ JQUANTS_API_KEY が設定されていません。")
        return

    # Qiita記事と公式V2仕様に基づいたヘッダー
    # Authorizationヘッダーに直接APIキーを入れる、または x-api-key を使用します
    # まずは標準的な Bearer 形式で試します
    headers = {
        "Authorization": f"Bearer {api_key}",
        "accept": "application/json"
    }
    
    try:
        target_code = "30480" # ビックカメラ
        print(f"🔄 J-Quants V2 APIで取得開始: {target_code}")
        
        # V2エンドポイント（URLに v2 が含まれる）
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}"
        res = requests.get(price_url, headers=headers, timeout=20)
        
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # V2ではカラム名が短縮されています（Date, Open, High, Low, Close, Volume等）
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = target_code[:4]
                df = df.rename(columns={
                    "Open":"open", "High":"high", "Low":"low", "Close":"price", "Volume":"volume"
                })
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ {target_code} のデータをDBへ保存しました！")
            else:
                print("⚠️ 応答データが空です。")
        else:
            print(f"❌ APIエラー: {res.status_code}")
            print(f"内容: {res.text[:200]}")

    except Exception as e:
        print(f"❌ 同期エラー: {e}")
