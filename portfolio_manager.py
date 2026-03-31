import os
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import database_manager

# -----------------------------------------------------------------------
# 設定項目
# -----------------------------------------------------------------------
BACKFILL_YEARS = 3  # ★ここを3年に設定

def get_target_tickers() -> dict:
    import io
    import requests
    from bs4 import BeautifulSoup
    base_url = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(list_page, headers=headers, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)
        if not link: return {"3048.T": {"name": "ビックカメラ"}}
        excel_url = base_url + link["href"]
        resp = requests.get(excel_url, headers=headers, timeout=60)
        df = pd.read_excel(io.BytesIO(resp.content))
        stock_map = {}
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip()
            name = str(row.iloc[2]).strip()
            if len(code) == 4 and code.isdigit():
                stock_map[f"{code}.T"] = {"name": name}
        return stock_map
    except Exception as e:
        print(f"❌ JPX銘柄マスタ取得失敗: {e}")
        return {"3048.T": {"name": "ビックカメラ"}}

# -----------------------------------------------------------------------
# メイン同期関数 (main.py から呼ばれる名前)
# -----------------------------------------------------------------------
def sync_data():
    db = database_manager.DBManager()
    
    # 1. ターゲット銘柄のリスト取得
    ticker_map = get_target_tickers()
    all_tickers = list(ticker_map.keys())
    
    # 2. 期間設定 (3年前 〜 今日)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=BACKFILL_YEARS * 365)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"🚀 爆速3年分バックフィル開始: {start_str} 〜 {end_str}")
    print(f"📊 対象: {len(all_tickers)} 銘柄")

    # 3. 10銘柄ずつのバッチ処理
    batch_size = 10 
    
    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i : i + batch_size]
        print(f"📦 [{i}/{len(all_tickers)}] {batch[0]}... 取得中")

        try:
            # yfinanceで3年分を一括取得
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
                # yfinanceの結果から該当銘柄のDFを取り出す
                if len(batch) == 1:
                    df_t = data.dropna(how="all")
                else:
                    if ticker not in data.columns.levels[0]: continue
                    df_t = data[ticker].dropna(how="all")
                
                for timestamp, row in df_t.iterrows():
                    if pd.isna(row.get("Close")): continue
                    
                    records.append({
                        "ticker": ticker.replace(".T", ""),
                        "date": timestamp.date(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "price": float(row["Close"]),
                        "volume": int(row["Volume"])
                    })
            
            # まとめてDB保存（database_manager側で重複は自動無視される前提）
            if records:
                db.save_prices(pd.DataFrame(records))
                print(f"✅ {len(batch)}銘柄分 ({len(records)}件) 保存")

        except Exception as e:
            print(f"⚠️ エラー発生 (Batch {i}): {e}")
            time.sleep(5)
        
        # Yahooへの負荷軽減
        time.sleep(1.2)

    print("\n🎉 3年分のヒストリカルデータ同期がすべて完了しました！")

if __name__ == "__main__":
    sync_data()
