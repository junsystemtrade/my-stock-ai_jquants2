import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    db = DBManager()
    
    # 環境変数を取得し、空白・改行・不要な引用符を徹底除去
    mail = os.environ.get("JQUANTS_MAIL", "").strip().strip("'").strip('"')
    password = os.environ.get("JQUANTS_PASSWORD", "").strip().strip("'").strip('"')
    
    if not mail or not password:
        print("❌ Error: Credentials are missing in environment variables.")
        return

    try:
        # 400 Bad Request対策：明示的なリフレッシュトークン更新を挟まない標準的な初期化
        print(f"🔍 J-Quants Login Attempt: {mail[:3]}***")
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        # 接続確認を兼ねて銘柄取得
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ {len(tickers)} 銘柄のリストを取得しました。")
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        return

    print(f"🔄 同期開始（上位500銘柄）...")
    success_count = 0
    for ticker in tickers[:500]:
        try:
            last_date = db.get_last_date(ticker)
            start = (last_date + timedelta(days=1)) if last_date else None
            
            df = yf.download(ticker, start=start, period="5y" if not last_date else None, progress=False)
            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
        except:
            continue
            
    print(f"✨ 同期完了！ {success_count} 銘柄のデータを更新しました。")

if __name__ == "__main__":
    sync_data()
