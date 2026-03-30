import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    # ❗ strip() で前後の空白を完全に除去
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    
    # 公式ドキュメントに準拠した厳格なヘッダー
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }
    
    # 3048 (ビックカメラ) / 2025-12-01 (Freeプラン確定圏内)
    code = "3048"
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={code}&from=2025-12-01&to=2025-12-01"
    
    print(f"📡 J-Quants V2 最終アタック開始...")

    try:
        # allow_redirects=False にして、リダイレクトされたら中身を見ずにエラーにする
        res = requests.get(url, headers=headers, timeout=20, allow_redirects=False)
        
        if res.status_code == 200:
            # もし中身がHTMLならここで弾く
            if "daily_quotes" not in res.text:
                print("❌ 認証は通ったようですが、返却されたのはJSONではありません。")
                return

            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                # ... (DB保存ロジック：前回と同じ) ...
                print(f"✨ ついに、ついに成功！J-Quants V2からデータが届きました。")
        else:
            print(f"❌ 認証失敗 (Status: {res.status_code})")
            print("💡 GitHub Secretsのキーに不要な文字（\" や空白）がないか今一度ご確認ください。")

    except Exception as e:
        print(f"❌ システムエラー: {e}")
