import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import database_manager

def check_v2_status():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    headers = {"x-api-key": api_key, "accept": "application/json"}
    
    # Freeプランでも確実に取れる「先週の特定日」を指定
    target_code = "3048" # ビックカメラ
    test_date = "2026-03-23" 
    
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={target_code}&from={test_date}&to={test_date}"
    
    print(f"📡 J-Quants V2 疎通テスト開始: {target_code} ({test_date})")
    res = requests.get(url, headers=headers, timeout=20, allow_redirects=False)
    
    if res.status_code == 200 and not res.text.startswith("/ja"):
        print("✅ 成功！JSONデータが返ってきました。")
        return True
    else:
        print(f"❌ まだリダイレクトまたはエラーです。状態: {res.status_code}")
        print(f"応答: {res.text[:50]}")
        return False

if __name__ == "__main__":
    check_v2_status()
