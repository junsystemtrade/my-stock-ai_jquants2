import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    """J-Quantsの銘柄リストに基づき、DBの株価データを最新に更新する"""
    db = DBManager()
    mail = os.getenv("JQUANTS_MAIL")
    password = os.getenv("JQUANTS_PASSWORD")
    
    try:
        # 引数名を 'mail' から 'user_email' に修正しました
        cli = jquantsapi.Client(user_email=mail, password=password)
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        return

    print(f"🔄 同期開始（上位500銘柄）...")
    for ticker in tickers[:500]:
        try:
            last_date = db.get_last_date(ticker)
            if last_date:
                start = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                if start >= datetime.now().strftime('%Y-%m-%d'): continue
                df = yf.download(ticker, start=start, progress=False)
            else:
                df = yf.download(ticker, period="5y", progress=False)
                
            if not df.empty:
                db.insert_prices(df, ticker)
        except Exception as e:
            continue
