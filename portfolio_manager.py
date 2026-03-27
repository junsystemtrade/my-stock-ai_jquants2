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
    
    # --- 1. J-Quants V2 APIから日本株を取得 ---
    print("🔍 J-Quants V2 API 接続開始...")
    
    # V2の正しいエンドポイントURL
    # ※全銘柄だと重いため、まずはビックカメラ(3048)などの個別指定からテストを推奨
    jq_url = "https://jpx-jquants.com/api/v2/prices/daily?code=30480" 
    
    try:
        res = requests.get(jq_url, headers=headers, timeout=30)
        res.raise_for_status()
        
        # V2のレスポンスキーは 'daily_quotes' です
        jq_list = res.json().get("daily_quotes", [])
        
        if jq_list:
            df_jq = pd.DataFrame(jq_list)
            # DBのカラム名に合わせてリネーム (例)
            df_jq = df_jq.rename(columns={
                "Code": "ticker",
                "Date": "date",
                "Close": "price",
                "Volume": "volume"
            })
            db.save_prices(df_jq)
            print(f"✅ J-Quants V2から {len(df_jq)} 件保存しました")
    except Exception as e:
        print(f"⚠️ J-Quants同期失敗: {e}")

    # --- 2. yfinance から米国株（AAPL, JMIA, NU）を補完 ---
    target_tickers = ["AAPL", "JMIA", "NU"] 
    print(f"🔍 yfinance補完開始: {target_tickers}")
    
    for symbol in target_tickers:
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
