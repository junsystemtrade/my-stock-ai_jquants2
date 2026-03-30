import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import database_manager

# --- J-Quants V2 差分取得ロジック ---
def sync_data():
    """GitHub Actionsから呼び出されるメイン関数"""
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # テスト対象（最終的には全銘柄リストをループさせます）
    target_tickers = ["3048", "8591"] 
    
    for code in target_tickers:
        print(f"🚀 {code} の同期を開始します...")
        # 1. まずは J-Quants V2 で最新差分を試みる
        success = fetch_jquants_v2(code, api_key, db)
        
        # 2. もしDBに5年分のデータがなければ yfinance で埋める（後日実装の全銘柄ループ用）
        # fetch_yfinance_5y(code, db) 

def fetch_jquants_v2(code, api_key, db):
    headers = {"x-api-key": api_key, "accept": "application/json"}
    # Freeプランでも取れる数日前のデータをターゲットに
    test_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={code}&from={test_date}&to={test_date}"
    
    try:
        res = requests.get(url, headers=headers, timeout=20, allow_redirects=False)
        if res.status_code == 200 and not res.text.startswith("/ja"):
            data = res.json()
            quotes = data.get("daily_quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['ticker'] = code
                df = df.rename(columns={"Open":"open", "High":"high", "Low":"low", "Close":"price", "Volume":"volume"})
                db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
                print(f"✅ J-Quants成功: {code}")
                return True
        print(f"⚠️ J-Quantsスキップ（リダイレクト等）: {code}")
        return False
    except Exception as e:
        print(f"❌ J-Quantsエラー: {e}")
        return False

if __name__ == "__main__":
    sync_data()
