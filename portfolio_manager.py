import os
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from database_manager import DBManager

def sync_data_3years():
    db = DBManager()
    
    # 1. 銘柄リストの取得
    from portfolio_manager import get_target_tickers
    ticker_map = get_target_tickers()
    all_tickers = list(ticker_map.keys())
    
    # 2. 期間設定 (3年前 〜 今日)
    BACKFILL_DAYS = 3 * 365
    end_date = datetime.now()
    start_date = end_date - timedelta(days=BACKFILL_DAYS)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"📅 3年分バックフィル開始: {start_str} 〜 {end_str}")
    print(f"📊 対象: {len(all_tickers)} 銘柄")

    # 3. 10銘柄ずつのバッチ処理
    # (Yahooの制限を考慮し、20より10の方が安定します)
    batch_size = 10 
    
    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i : i + batch_size]
        print(f"📦 [{i}/{len(all_tickers)}] {batch[0]}... 取得開始")

        try:
            # yfinanceで3年分を一括取得
            # threads=True で高速化
            data = yf.download(
                batch, 
                start=start_str, 
                end=end_str, 
                interval="1d", 
                group_by='ticker', 
                auto_adjust=True,
                progress=False,
                threads=True
            )

            records = []
            for ticker in batch:
                if ticker not in data: continue
                
                # 単一銘柄取得時と複数銘柄取得時でDataFrameの構造が変わるためケア
                if len(batch) == 1:
                    df_t = data.dropna(how="all")
                else:
                    df_t = data[ticker].dropna(how="all")
                
                for timestamp, row in df_t.iterrows():
                    # 欠損値チェック
                    if pd.isna(row["Close"]): continue
                    
                    records.append({
                        "ticker": ticker.replace(".T", ""),
                        "date": timestamp.date(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "price": float(row["Close"]),
                        "volume": int(row["Volume"])
                    })
            
            # DBへバルクインサート
            if records:
                db.save_prices(pd.DataFrame(records))
                print(f"✅ {len(batch)}銘柄分 ({len(records)}レコード) 保存完了")
            else:
                print(f"⚠️ {batch} のデータが見つかりませんでした")

        except Exception as e:
            print(f"❌ エラー発生 (Batch {i}): {e}")
            time.sleep(10) # エラー時は少し長めに待機
        
        # サーバーに優しく（村田さんのIPがBANされないように）
        time.sleep(1.5)

    print("\n✨ 3年間の全ヒストリカルデータ同期が完了しました！")

if __name__ == "__main__":
    sync_data_3years()
