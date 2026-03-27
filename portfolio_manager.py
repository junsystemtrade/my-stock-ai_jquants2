import os
import yfinance as yf
import jquantsapi
import pandas as pd
import time
from datetime import datetime, timedelta
from database_manager import DBManager

def clean_secret(value):
    """GitHub Secretsから渡される引用符や空白を、パスワード内の記号を壊さずに除去する"""
    if not value:
        return ""
    # 前後の空白を除去し、もし引用符で囲まれていたらそれだけを剥ぎ取る
    s = str(value).strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    # 改行コードなどの不可視文字を徹底排除
    return s.replace('\n', '').replace('\r', '')

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
        # Client初期化
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        # 400 Bad Request対策：明示的にリフレッシュトークンを取得
        cli.get_refresh_token()
        
        # 銘柄情報の取得
        listed_info = cli.get_listed_info()
        
        # 【改善】銘柄コードの整形（5桁対応：13010 -> 1301.T）
        tickers = []
        for code in listed_info['Code']:
            c = str(code)
            # 5桁の場合は先頭4桁、それ以外は0埋め4桁にして .T を付与
            code_4 = c[:4] if len(c) >= 4 else c.zfill(4)
            tickers.append(f"{code_4}.T")
            
        print(f"✅ J-Quants 認証成功！ {len(tickers)} 銘柄のリストを取得しました。")
    except Exception as e:
        print(f"❌ J-Quants認証失敗: {e}")
        return

    print(f"🔄 株価同期開始（上位500銘柄を処理）...")
    success_count = 0
    
    # 【改善】ループ内でのエラー可視化とレート制限対策
    for ticker in tickers[:500]:
        try:
            # DBから最終更新日を取得
            last_date = db.get_last_date(ticker)
            
            # 【改善】日付の型エラー回避（date型をdatetime型に安全に変換）
            if last_date:
                start_date = datetime.combine(last_date, datetime.min.time()) + timedelta(days=1)
            else:
                start_date = datetime.now() - timedelta(days=365*5)
            
            # Yahoo Financeからデータ取得（タイムアウト設定を追加）
            df = yf.download(ticker, start=start_date, progress=False, timeout=15)
            
            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
                # 【改善】Yahooへの負荷軽減（0.5秒待機）
                time.sleep(0.5)
            else:
                # ログをスッキリさせるため、データなしは1行のみ表示
                pass
                
        except Exception as e:
            # 【改善】エラーが起きた銘柄を特定できるようにする
            print(f"⚠️ Error on {ticker}: {e}")
            continue
            
    print(f"✨ 同期完了！合計 {success_count} 銘柄のデータを更新しました。")

if __name__ == "__main__":
    sync_data()
