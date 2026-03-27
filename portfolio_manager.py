import os
import requests
import yfinance as yf
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    
    print("🔍 J-Quants V2 API 接続開始...")
    
    # 銘柄コードは5桁(例: 30480)で指定する必要があります
    jq_url = "https://jpx-jquants.com/api/v2/prices/daily?code=30480" 
    
    try:
        res = requests.get(jq_url, headers=headers, timeout=30)
        if res.status_code == 200:
            data = res.json()
            # V2のキー名は 'daily_quotes'
            jq_list = data.get("daily_quotes", [])
            if jq_list:
                df_jq = pd.DataFrame(jq_list)
                # DBのカラム名に合わせてリネーム
                df_jq = df_jq.rename(columns={
                    "Code": "ticker", "Date": "date", "Close": "price", "Volume": "volume"
                })
                db.save_prices(df_jq)
                print(f"✅ J-Quantsから {len(df_jq)} 件保存")
        else:
            print(f"⚠️ J-Quants API応答なし: {res.status_code}")
    except Exception as e:
        print(f"❌ J-Quantsエラー: {e}")

    # yfinance補完
    for symbol in ["AAPL", "JMIA", "NU"]:
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
            print(f"❌ {symbol} DB保存失敗: {e}")
