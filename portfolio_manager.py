import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    db = DBManager()
    mail = os.getenv("JQUANTS_MAIL")
    password = os.getenv("JQUANTS_PASSWORD")
    
    try:
        # ID/PWで永続的に自動ログイン
        cli = jquantsapi.Client(mail=mail, password=password)
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
    except Exception as e:
        print(f"❌ J-Quantsログインエラー: {e}")
        return

    print(f"🔄 同期開始（全{len(tickers)}銘柄中、上位500件を優先）...")
    for ticker in tickers[:500]:
        try:
            last_date = db.get_last_date(ticker)
            if last_date:
                start = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                if start >= datetime.now().strftime('%Y-%m-%d'): continue
                df = yf.download(ticker, start=start, progress=False)
            else:
                df = yf.download(ticker, period="5y", progress=False)
            if not df.empty: db.insert_prices(df, ticker)
        except: continue
