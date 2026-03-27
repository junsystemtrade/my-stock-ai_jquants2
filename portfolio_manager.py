import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 2026年V2仕様：x-api-key のみで、他の余計な情報を排除
    headers = {
        "x-api-key": api_key,
        "accept": "application/json"
    }
    
    try:
        target_code = "30480"
        # 確実にFreeプランで許可されている「1週間前」を指定
        test_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        print(f"🕵️ サーバーの正体を暴きます (Key: {api_key[:5]}...)")
        price_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}&from={test_date}&to={test_date}"
        
        # あえてリダイレクトを許可し、何が返ってくるか見る
        res = requests.get(price_url, headers=headers, timeout=20)
        
        print(f"📡 ステータスコード: {res.status_code}")
        print(f"📡 応答内容（先頭200文字）: \n{res.text[:200]}")

        # ここで強引にJSON解析を試みる
        if res.status_code == 200:
            try:
                data = res.json()
                print("✅ 奇跡的にJSON解析に成功しました！")
            except:
                print("❌ JSONではありません。上記HTMLの中にエラー理由が書いてあるはずです。")

    except Exception as e:
        print(f"❌ 通信エラー: {e}")

if __name__ == "__main__":
    sync_data()
