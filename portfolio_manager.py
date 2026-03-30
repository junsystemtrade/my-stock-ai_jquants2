import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    
    # 最終的には全銘柄ですが、まずはテスト対象
    target_tickers = ["3048", "8591"] 
    
    for code in target_tickers:
        print(f"🚀 {code} の同期を開始します...")
        
        # 1. J-Quants V2 を試行（最新の差分用）
        success = fetch_jquants_v2(code, api_key, db)
        
        # 2. J-Quantsがスキップされた場合や、過去5年分を埋めるために yfinance を使用
        if not success:
            print(f"🔄 J-Quantsがスキップされたため、yfinanceで補完します: {code}")
            fetch_yfinance_history(code, db)

def fetch_jquants_v2(code, api_key, db):
    headers = {"x-api-key": api_key, "accept": "application/json"}
    # Freeプランの制限を考慮し、あえて「1週間前」の1日分を狙い撃ち
    target_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    url = f"https://jpx-jquants.com/api/v2/prices/daily?code={code}&from={target_date}&to={target_date}"
    
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
                print(f"✅ J-Quantsデータ取得成功")
                return True
        return False
    except:
        return False

def fetch_yfinance_history(code, db):
    """Yahoo Financeから直近のデータを取得してDBの穴を埋める"""
    try:
        symbol = f"{code}.T"
        ticker_obj = yf.Ticker(symbol)
        # とりあえず直近1ヶ月分を取得（5年分は後で一括で行うため）
        df = ticker_obj.history(period="1mo")
        if not df.empty:
            df = df.reset_index()
            df['date'] = df['Date'].dt.date
            df['ticker'] = code
            df = df.rename(columns={"Open":"open", "High":"high", "Low":"low", "Close":"price", "Volume":"volume"})
            db.save_prices(df[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']])
            print(f"✅ yfinance補完成功: {len(df)}件")
    except Exception as e:
        print(f"❌ yfinance補完エラー: {e}")
