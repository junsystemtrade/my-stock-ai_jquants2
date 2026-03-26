import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    db = DBManager()
    
    # .strip() を追加して、前後の空白や改行を完全に除去
    mail = os.getenv("JQUANTS_MAIL", "").strip()
    password = os.getenv("JQUANTS_PASSWORD", "").strip()
    
    if not mail or not password:
        print("❌ Secrets error: JQUANTS_MAIL or PASSWORD is empty")
        return

    try:
        # 認証実行
        cli = jquantsapi.Client(mail_address=mail, password=password)
        print(f"🔍 J-Quants Login Success: {mail[:3]}***")
        
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        return

    print(f"🔄 同期開始（上位500銘柄）...")
    success_count = 0
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
                success_count += 1
        except:
            continue
            
    print(f"✨ 同期完了！ {success_count} 銘柄更新")

if __name__ == "__main__":
    sync_data()
