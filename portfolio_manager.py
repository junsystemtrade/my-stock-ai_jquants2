import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    db = DBManager()
    
    # getenv("") ではなく os.environ.get("") を使い、末尾の改行・空白を徹底除去
    mail = os.environ.get("JQUANTS_MAIL", "").strip()
    password = os.environ.get("JQUANTS_PASSWORD", "").strip()
    
    if not mail or not password:
        print("❌ Secrets error: JQUANTS_MAIL or PASSWORD is empty")
        return

    try:
        # 認証実行：内部でのリトライを考慮し、refresh_tokenを明示的に求めない設定
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        # 400エラー対策：ここで銘柄一覧を取得する前に、接続テストが行われます
        print(f"🔍 J-Quants Login Attempt: {mail[:3]}***")
        listed_info = cli.get_listed_info()
        
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ {len(tickers)} 銘柄のリストを取得しました。")
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        return

    # --- 以下、ダウンロード処理 ---
    print(f"🔄 同期開始（上位500銘柄）...")
    success_count = 0
    for ticker in tickers[:500]:
        try:
            last_date = db.get_last_date(ticker)
            df = yf.download(ticker, start=(last_date + timedelta(days=1)) if last_date else None, period="5y" if not last_date else None, progress=False)
            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
        except: continue
    print(f"✨ 同期完了！ {success_count} 銘柄更新")
