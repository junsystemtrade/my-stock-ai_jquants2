import os
import yfinance as yf
import jquantsapi
import pandas as pd
import time
from datetime import datetime, timedelta
from database_manager import DBManager

def clean_secret(value):
    """前後の引用符と改行のみを除去し、パスワード内の記号は保護する"""
    if not value:
        return ""
    return str(value).strip().strip("'").strip('"').replace('\n', '').replace('\r', '')

def sync_data():
    db = DBManager()
    
    mail = clean_secret(os.environ.get("JQUANTS_MAIL"))
    password = clean_secret(os.environ.get("JQUANTS_PASSWORD"))
    
    if not mail or not password:
        print("❌ Error: Credentials not found.")
        return

    try:
        print(f"🔍 J-Quants Login: {mail[:3]}***")
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        # 銘柄リスト取得
        listed_info = cli.get_listed_info()
        
        # 【改善】5桁コード対応 & 0埋め4桁整形
        # J-QuantsのCodeが13010の場合、int(13010/10) -> 1301 -> "1301.T"
        tickers = []
        for code in listed_info['Code']:
            c = str(code)
            code_4 = c[:4] if len(c) >= 4 else c.zfill(4)
            tickers.append(f"{code_4}.T")
            
        print(f"✅ J-Quants 認証成功！ {len(tickers)} 銘柄取得")
    except Exception as e:
        print(f"❌ J-Quants認証失敗: {e}")
        return

    print(f"🔄 同期開始（上位500銘柄）...")
    success_count = 0
    
    # 【改善】1件ずつ処理（Yahooのレート制限対策として微小なスリープを挿入）
    for ticker in tickers[:500]:
        try:
            last_date = db.get_last_date(ticker)
            
            # 型の不一致を防ぐため、確実にdatetime.datetimeに変換
            if last_date:
                start_date = datetime.combine(last_date, datetime.min.time()) + timedelta(days=1)
            else:
                start_date = datetime.now() - timedelta(days=365*5)
            
            # progress=Falseでログをスッキリさせる
            df = yf.download(ticker, start=start_date, progress=False, timeout=10)
            
            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
                # 【改善】APIへの負荷軽減（0.5秒待機）
                time.sleep(0.5) 
            else:
                print(f"⚠️ No new data for {ticker}")
                
        except Exception as e:
            # 【改善】エラー内容を可視化
            print(f"❌ Error on {ticker}: {e}")
            continue
            
    print(f"✨ 同期完了！ {success_count} 銘柄更新済")

if __name__ == "__main__":
    sync_data()
