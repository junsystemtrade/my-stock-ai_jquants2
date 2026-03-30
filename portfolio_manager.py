import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    
    # 🎯 クイックスタートで判明した正しいエンドポイント
    base_url = "https://api.jquants.com/v2/equities/bars/daily"
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    
    # テスト銘柄: 30480 (ビックカメラ)
    params = {"code": "30480", "date": "20251201"}
    
    print(f"🎯 J-Quants V2 データ抽出開始: {params['code']}")

    try:
        res = requests.get(base_url, headers=headers, params=params, timeout=20)
        
        if res.status_code == 200:
            raw_data = res.json()
            # ❗重要: 'bars' ではなく 'data' キーから取得します
            quotes = raw_data.get("data", [])
            
            if quotes:
                df = pd.DataFrame(quotes)
                
                # J-Quants V2 (data形式) のカラム名をDB用に変換
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = "3048"
                df = df.rename(columns={
                    "O": "open", "H": "high", "Low": "low", # Lの場合もあり
                    "C": "price", "Vo": "volume"
                })
                # 万が一 'L' が 'low' になっていない場合の補完
                if 'L' in df.columns: df = df.rename(columns={"L": "low"})
                
                # Supabaseに保存！
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✨【完全勝利】Supabaseへの保存に成功しました！")
            else:
                print(f"⚠️ データが空でした。レスポンス: {raw_data}")
        else:
            print(f"❌ 通信エラー: {res.status_code}")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
