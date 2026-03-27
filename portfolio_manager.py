import os
import yfinance as yf
import jquantsapi
import pandas as pd
from datetime import datetime, timedelta
from database_manager import DBManager

def clean_secret(value):
    """GitHub Secretsから渡される引用符や空白を完全に除去する"""
    if not value:
        return ""
    # 前後の空白、シングルクォート、ダブルクォート、改行をすべて削除
    return str(value).strip().strip("'").strip('"').replace('\n', '').replace('\r', '')

def sync_data():
    db = DBManager()
    
    # 環境変数の取得とクリーンアップ
    mail = clean_secret(os.environ.get("JQUANTS_MAIL"))
    password = clean_secret(os.environ.get("JQUANTS_PASSWORD"))
    
    if not mail or not password:
        print("❌ Error: JQUANTS_MAIL or JQUANTS_PASSWORD is not set.")
        return

    try:
        print(f"🔍 J-Quants Login Attempt: {mail[:3]}***")
        # Client初期化（refresh_tokenを指定せず標準構成で開始）
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        # 400 Bad Request対策：明示的にリフレッシュトークンを更新
        cli.get_refresh_token()
        
        # 銘柄情報の取得
        listed_info = cli.get_listed_info()
        # 銘柄コードを4桁に整形してYahoo Finance形式 (.T) に変換
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ J-Quants 認証成功！ {len(tickers)} 銘柄のリストを取得しました。")
    except Exception as e:
        print(f"❌ J-Quants認証失敗: {e}")
        if "400" in str(e):
            print("💡 アドバイス: J-Quantsのパスワード自体に記号がある場合、Secret設定時に引用符で囲んでいないか再確認してください。")
        return

    print(f"🔄 株価同期開始（上位500銘柄を処理）...")
    success_count = 0
    
    # 計算負荷とAPI制限を考慮し、上位500件を対象に更新
    for ticker in tickers[:500]:
        try:
            # DBから該当銘柄の最終更新日を取得
            last_date = db.get_last_date(ticker)
            
            # 取得開始日の設定（データがあれば翌日から、なければ5年前から）
            start_date = (last_date + timedelta(days=1)) if last_date else (datetime.now() - timedelta(days=365*5))
            
            # Yahoo Financeからデータ取得
            df = yf.download(ticker, start=start_date, progress=False)
            
            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
        except Exception as e:
            # 個別銘柄のエラーはスキップして継続
            continue
            
    print(f"✨ 同期完了！合計 {success_count} 銘柄のデータを更新しました。")

if __name__ == "__main__":
    sync_data()
