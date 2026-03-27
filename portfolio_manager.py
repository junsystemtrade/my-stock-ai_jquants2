import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    db = DBManager()
    
    # os.getenvではなくos.environから直接取得し、不要な文字を徹底排除
    mail = os.environ.get("JQUANTS_MAIL", "").strip().strip("'").strip('"')
    password = os.environ.get("JQUANTS_PASSWORD", "").strip().strip("'").strip('"')
    
    if not mail or not password:
        print("❌ Error: JQUANTS_MAIL or JQUANTS_PASSWORD is not set in Secrets.")
        return

    try:
        # 認証オブジェクト作成
        # refresh_tokenのエラーを避けるため、Clientを最小構成で初期化
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        print(f"🔍 J-Quants Login Attempt for: {mail[:3]}***")
        # 銘柄情報の取得（ここで内部的にトークンが発行される）
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ {len(tickers)} 銘柄のリストを取得に成功しました！")
        
    except Exception as e:
        print(f"❌ J-Quants認証失敗: {e}")
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

if __name__ == "__main__":
    sync_data()
