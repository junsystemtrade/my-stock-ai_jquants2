import os
import yfinance as yf
import jquantsapi
import re
from datetime import datetime, timedelta
from database_manager import DBManager

def clean_secret(value):
    """GitHub Secretsから渡される可能性のあるノイズを完全に除去する"""
    if not value:
        return ""
    # 前後の空白、改行、シングルクォート、ダブルクォートをすべて削除
    cleaned = value.strip().strip("'").strip('"')
    return cleaned

def sync_data():
    db = DBManager()
    
    # Secretsの「毒抜き」
    mail = clean_secret(os.environ.get("JQUANTS_MAIL"))
    password = clean_secret(os.environ.get("JQUANTS_PASSWORD"))
    
    if not mail or not password:
        print("❌ Error: Credentials are missing in environment variables.")
        return

    try:
        # J-Quantsログイン試行
        print(f"🔍 J-Quants Login Attempt: {mail[:3]}***")
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        # ここでトークン取得を強制（400エラーが出るならここで落ちる）
        listed_info = cli.get_listed_info()
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ {len(tickers)} 銘柄のリストを取得しました。")
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        # ヒントを表示
        if "400" in str(e):
            print("💡 400エラー: パスワードに特殊記号（&や$など）が含まれる場合、Secretの設定時に引用符で囲まず、ベタ書きしているか再確認してください。")
        return

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
