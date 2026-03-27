import os
import yfinance as yf
import jquantsapi
import pandas as pd
import time
from datetime import datetime, timedelta
from database_manager import DBManager

def clean_secret(value):
    """GitHub Secrets の引用符や不可視文字を安全に除去"""
    if not value:
        return ""
    s = str(value).strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    return s.replace("\n", "").replace("\r", "")

def sync_data():
    db = DBManager()

    mail = clean_secret(os.environ.get("JQUANTS_MAIL"))
    password = clean_secret(os.environ.get("JQUANTS_PASSWORD"))

    if not mail or not password:
        print("❌ Error: J-Quants credentials missing.")
        return

    # -------------------------
    # J-Quants 認証
    # -------------------------
    try:
        print(f"🔍 J-Quants Login Attempt: {mail[:3]}***")
        cli = jquantsapi.Client(mail_address=mail, password=password)
        cli.get_refresh_token()

        listed_info = cli.get_listed_info()

        # 5桁コード → 4桁化（13010 → 1301.T）
        tickers = []
        for code in listed_info["Code"]:
            c = str(code)
            code_4 = str(int(c))[:4]  # Copilot推奨の安全版
            tickers.append(f"{code_4}.T")

        print(f"✅ J-Quants 認証成功！ {len(tickers)} 銘柄取得")
    except Exception as e:
        print(f"❌ J-Quants認証失敗: {e}")
        return

    # -------------------------
    # 株価同期
    # -------------------------
    print("🔄 株価同期開始（上位500銘柄）...")
    success_count = 0

    for ticker in tickers[:500]:
        try:
            last_date = db.get_last_date(ticker)

            if last_date:
                start_date = datetime.combine(last_date, datetime.min.time()) + timedelta(days=1)
            else:
                start_date = datetime.now() - timedelta(days=365 * 5)

            df = yf.download(ticker, start=start_date, progress=False)

            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
                time.sleep(0.1)  # Copilot推奨：0.5秒 → 0.1秒で十分
        except Exception as e:
            print(f"⚠️ Error on {ticker}: {type(e).__name__}: {e}")
            continue

    print(f"✨ 同期完了！ {success_count} 銘柄更新済")

if __name__ == "__main__":
    sync_data()
