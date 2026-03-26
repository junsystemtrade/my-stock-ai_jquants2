import os
import yfinance as yf
import jquantsapi
import json
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    db = DBManager()
    
    # getenvのデフォルト値を空文字にして、Noneエラーを防ぐ
    mail = os.getenv("JQUANTS_MAIL", "")
    password = os.getenv("JQUANTS_PASSWORD", "")
    
    if not mail or not password:
        print("❌ エラー: GitHub Secretsの JQUANTS_MAIL / PASSWORD が空です。")
        return

    try:
        # パスワードの特殊文字問題を避けるため、内部で一度json.dumpsに近い処理を通す
        # 最新の jquantsapi 仕様で初期化
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        print(f"🔍 J-Quantsログイン試行中... (ID: {mail[:3]}***)")
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ {len(tickers)} 銘柄のリストを取得しました。")
        
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        return

    # --- 以下、ダウンロード処理は変更なし ---
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
    print(f"✨ 同期完了！ {success_count} 銘柄更新。")

if __name__ == "__main__":
    sync_data()
