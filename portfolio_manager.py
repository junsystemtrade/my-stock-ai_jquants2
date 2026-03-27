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
    
    # --- 1. J-Quants V2 から日本株データを取得 ---
    print("🔍 J-Quants V2 APIから日本株を取得中...")
    # 例として直近の日次データを取得（エンドポイントはV2専用）
    jq_url = "https://api.jpx-jquants.com/v2/prices/daily"
    res = requests.get(jq_url, headers=headers)
    
    if res.status_code == 200:
        jq_data = res.json().get("daily_prices", [])
        if jq_data:
            df_jq = pd.DataFrame(jq_data)
            # DBの型に合わせる整形処理（適宜調整）
            db.save_prices(df_jq)
            print(f"✅ J-Quantsから {len(df_jq)} 件保存しました")
    else:
        print(f"⚠️ J-Quants APIエラー: {res.status_code}")

    # --- 2. yfinance から不足データ（米国株など）を補完 ---
    # 村田さんが保有・注目している銘柄リスト
    tickers = ["AAPL", "JMIA", "NU", "3048.T"] # 3048.Tはビックカメラ
    print(f"🔍 yfinanceから {tickers} を取得中...")
    
    for symbol in tickers:
        try:
            ticker_data = yf.Ticker(symbol).history(period="2d")
            if not ticker_data.empty:
                # DB保存用の整形
                latest = ticker_data.tail(1)
                df_yf = pd.DataFrame({
                    "ticker": [symbol.replace(".T", "")],
                    "date": [latest.index[0].date()],
                    "price": [latest["Close"].values[0]],
                    "volume": [latest["Volume"].values[0]]
                })
                db.save_prices(df_yf)
                print(f"✅ {symbol} のデータを保存しました")
        except Exception as e:
            print(f"❌ {symbol} の取得失敗: {e}")
