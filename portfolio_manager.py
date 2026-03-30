import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }
    
    # ❗2025-12-01(月) 確実に開場していた日を指定
    target_date = "2025-12-01"
    code = "3048" # ビックカメラ
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={code}&from={target_date}&to={target_date}"
    
    print(f"📡 J-Quants V2 営業日アタック: {code} ({target_date})")

    try:
        res = requests.get(url, headers=headers, timeout=20)
        
        # デバッグ：中身が何かを少しだけ表示
        print(f"Status: {res.status_code}")
        print(f"Response Preview: {res.text[:100]}")

        if res.status_code == 200:
            # HTMLが混じっていないか最終チェック
            if "<html" in res.text.lower():
                print("❌ まだHTMLが返っています。Freeプランの制限（12週間前）に抵触している可能性があります。")
                return

            data = res.json()
            quotes = data.get("daily_quotes", [])
            
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = code
                df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"price","Volume":"volume"})
                
                # 開通済みのSupabaseへ保存
                db.save_prices(df[['ticker','date','open','high','low','price','volume']])
                print(f"✨【完遂】J-Quants V2 からのデータ着弾・DB保存に成功しました！")
            else:
                print(f"⚠️ 認証成功ですが、{target_date}のデータが空です。日付を前後させてみてください。")
        else:
            print(f"❌ 通信エラー: {res.status_code}")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
