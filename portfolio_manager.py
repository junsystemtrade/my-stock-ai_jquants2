import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    
    # ❗【重要】最新のAPI専用エンドポイントに修正
    # クイックスタートの指示通り api.jquants.com を使用します
    base_url = "https://api.jquants.com/v2/equities/bars/daily"
    
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }
    
    # クイックスタートの例に倣い、86970 (日本取引所) または 30480 (5桁) を試します
    # Freeプランの「12週間の壁」を考慮し、20251201 形式で指定
    params = {
        "code": "30480",  # V2は5桁（後ろに0）が必要な場合があります
        "date": "20251201"
    }
    
    print(f"🎯 真・J-Quants V2 アタック: {params['code']} ({params['date']})")

    try:
        # curl -G と同じ挙動にするため params を使用
        res = requests.get(base_url, headers=headers, params=params, timeout=20)
        
        print(f"📡 Status: {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            # V2のレスポンス構造（bars）に合わせて取得
            bars = data.get("bars", [])
            
            if bars:
                df = pd.DataFrame(bars)
                # カラム名変換（J-Quants V2の仕様に合わせる）
                # 例: Date -> date, Open -> open など
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = "3048"
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", 
                    "Close": "price", "Volume": "volume"
                })
                
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✨【歴史的勝利】J-Quants V2 正式APIからのデータ着弾に成功！")
            else:
                print(f"⚠️ 接続成功ですがデータが空です。レスポンス: {data}")
        else:
            print(f"❌ エラー: {res.status_code} / {res.text}")

    except Exception as e:
        print(f"❌ 実行失敗: {e}")
