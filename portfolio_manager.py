import os
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # --- 1. J-Quants V2 APIから日本株データを取得 ---
    print("🔍 J-Quants V2 API 接続開始...")
    # V2の日次株価エンドポイント
    jq_url = "https://api.jpx-jquants.com/v2/prices/daily"
    res = requests.get(jq_url, headers=headers)
    
    if res.status_code == 200:
        raw_json = res.json()
        # V2のレスポンス構造に合わせて抽出（構造は公式ドキュメント準拠）
        jq_list = raw_json.get("daily_prices", [])
        if jq_list:
            df_jq = pd.DataFrame(jq_list)
            # DBカラム名に合わせた整形（ticker, date, price, volume等）
            # ※J-Quants V2のキー名に合わせて適宜renameしてください
            db.save_prices(df_jq)
            print(f"✅ J-Quantsから {len(df_jq)} 件保存しました")
    else:
        print(f"⚠️ J-Quants V2エラー: {res.status_code} {res.text}")

    # --- 2. yfinance から注目銘柄（米国株・日本株個別）を補完 ---
    # 村田さんのポートフォリオ銘柄
    target_tickers = ["AAPL", "JMIA", "NU", "3048.T"] 
    print(f"🔍 yfinance補完開始: {target_tickers}")
    
    for symbol in target_tickers:
        try:
            ticker_obj = yf.Ticker(symbol)
            hist = ticker_obj.history(period="5d") # 直近5日分
            if not hist.empty:
                latest = hist.tail(1)
                df_yf = pd.DataFrame({
                    "ticker": [symbol.replace(".T", "")],
                    "date": [latest.index[0].date()],
                    "price": [float(latest["Close"].values[0])],
                    "volume": [int(latest["Volume"].values[0])]
                })
                db.save_prices(df_yf)
                print(f"✅ {symbol} 同期完了")
        except Exception as e:
            print(f"❌ {symbol} 同期失敗: {e}")

if __name__ == "__main__":
    sync_data()
