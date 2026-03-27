import os
import requests
import yfinance as yf
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # --- 1. J-Quants V2 API (日本株: ビックカメラ 30480 等) ---
    print("🔍 J-Quants V2 API 同期開始...")
    # V2の正しいURL形式。codeは5桁で指定
    jq_url = "https://jpx-jquants.com/api/v2/prices/daily?code=30480" 
    
    try:
        res = requests.get(jq_url, headers=headers, timeout=30)
        if res.status_code == 200:
            data = res.json()
            jq_list = data.get("daily_quotes", []) # V2のキー名は daily_quotes
            if jq_list:
                df_jq = pd.DataFrame(jq_list)
                # DBのカラム名に合わせてリネーム
                df_jq = df_jq.rename(columns={
                    "Code": "ticker", "Date": "date", "Close": "price", "Volume": "volume"
                })
                db.save_prices(df_jq)
        else:
            print(f"⚠️ J-Quants APIエラー: 状態コード {res.status_code}")
    except Exception as e:
        print(f"❌ J-Quants同期失敗: {e}")

    # --- 2. yfinance (米国株: AAPL, JMIA, NU) ---
    tickers = ["AAPL", "JMIA", "NU"]
    print(f"🔍 yfinance 補完開始: {tickers}")
    
    for symbol in tickers:
        try:
            hist = yf.Ticker(symbol).history(period="2d")
            if not hist.empty:
                latest = hist.tail(1)
                df_yf = pd.DataFrame({
                    "ticker": [symbol],
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
