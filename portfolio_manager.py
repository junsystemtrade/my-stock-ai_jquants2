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
        # ID/PWによる自動ログイン（Refresh Tokenの手動更新が不要な方式）
        cli = jquantsapi.Client(mail=mail, password=password)
        listed_info = cli.get_listed_info()
        # 銘柄コードを yfinance 形式（例: 7203.T）に変換
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        return

    print(f"🔄 同期開始（API制限回避のため上位500銘柄を処理）...")
    for ticker in tickers[:500]:
        try:
            # DBに保存されている最新の日付を確認
            last_date = db.get_last_date(ticker)
            
            if last_date:
                # すでにデータがある場合は、翌日から今日までの差分を取得
                start = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                if start >= datetime.now().strftime('%Y-%m-%d'): continue
                df = yf.download(ticker, start=start, progress=False)
            else:
                # データがない場合は新規で5年分取得
                df = yf.download(ticker, period="5y", progress=False)
                
            if not df.empty:
                db.insert_prices(df, ticker)
        except Exception as e:
            print(f"⚠️ {ticker} の取得に失敗: {e}")
            continue
