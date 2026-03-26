import os
import yfinance as yf
import jquantsapi
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data():
    """J-Quantsの銘柄リストに基づき、DBの株価データを最新に更新する"""
    db = DBManager()
    
    # GitHub Secretsから環境変数を取得
    mail = os.getenv("JQUANTS_MAIL")
    password = os.getenv("JQUANTS_PASSWORD")
    
    # 認証情報の存在確認（デバッグ用）
    if not mail or not password:
        print("❌ エラー: JQUANTS_MAIL または JQUANTS_PASSWORD が設定されていません。")
        return

    try:
        # 最新のライブラリ仕様(mail_address)に合わせて明示的に認証
        # 2段階認証不要のAPIキー運用に近い挙動をさせます
        cli = jquantsapi.Client(mail_address=mail, password=password)
        
        print("🔍 J-Quantsから銘柄リストを取得中...")
        listed_info = cli.get_listed_info()
        
        # 銘柄コードを yfinance 形式 (xxxx.T) に変換
        tickers = [f"{str(code)[:4]}.T" for code in listed_info['Code']]
        print(f"✅ {len(tickers)} 銘柄のリストを取得しました。")
        
    except Exception as e:
        print(f"❌ J-Quants認証エラー: {e}")
        print("※パスワードに特殊記号が含まれる場合、Secretの設定を再確認してください。")
        return

    # API制限と処理時間を考慮し、上位銘柄から同期（まずは500銘柄）
    print(f"🔄 同期開始（上位500銘柄）...")
    success_count = 0
    
    for ticker in tickers[:500]:
        try:
            # DB内の最新日付を確認
            last_date = db.get_last_date(ticker)
            
            if last_date:
                # データの続きから取得
                start = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                if start >= datetime.now().strftime('%Y-%m-%d'):
                    continue
                df = yf.download(ticker, start=start, progress=False)
            else:
                # 初回は過去5年分を取得
                df = yf.download(ticker, period="5y", progress=False)
                
            if not df.empty:
                db.insert_prices(df, ticker)
                success_count += 1
                if success_count % 50 == 0:
                    print(f"進捗: {success_count} 銘柄完了...")
                    
        except Exception as e:
            # 個別銘柄のエラーはスキップして継続
            continue

    print(f"✨ 同期完了！ {success_count} 銘柄のデータを更新しました。")

if __name__ == "__main__":
    sync_data()
