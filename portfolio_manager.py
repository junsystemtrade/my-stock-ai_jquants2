import os
import requests
import yfinance as yf
import pandas as pd
import database_manager

def get_id_token():
    """リフレッシュトークンからIDトークンを取得する"""
    refresh_token = os.getenv("JQUANTS_API_KEY")
    print("🔑 IDトークンを取得中...")
    url = f"https://jpx-jquants.com/api/v2/token/auth_refresh?refreshtoken={refresh_token}"
    res = requests.post(url)
    res.raise_for_status()
    return res.json().get("idToken")

def sync_data():
    db = database_manager.DBManager()
    
    # 1. 認証してIDトークンを取得
    try:
        id_token = get_id_token()
        headers = {"Authorization": f"Bearer {id_token}"}
    except Exception as e:
        print(f"❌ 認証失敗: {e}")
        return

    # 2. 銘柄リスト取得
    print("🔍 J-Quants V2 から上場銘柄リストを取得中...")
    list_url = "https://jpx-jquants.com/api/v2/listed/info"
    res = requests.get(list_url, headers=headers)
    
    if res.status_code != 200:
        print(f"❌ リスト取得失敗: {res.status_code} {res.text}")
        return
        
    df_info = pd.DataFrame(res.json().get("info", []))
    # テストのため、まずはビックカメラ(3048)と任天堂(7974)など数件に絞る
    test_codes = ["30480", "79740"] 
    
    for code in test_codes:
        yf_ticker = f"{code[:4]}.T"
        print(f"🔄 同期中: {yf_ticker}")
        
        # yfinanceで直近データを取得
        df_yf = yf.download(yf_ticker, period="5d", interval="1d", progress=False, auto_adjust=True)
        if not df_yf.empty:
            if isinstance(df_yf.columns, pd.MultiIndex):
                df_yf.columns = df_yf.columns.get_level_values(0)
            
            df_yf['ticker'] = code[:4]
            df_yf['date'] = df_yf.index.date
            df_yf = df_yf.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"})
            
            db.save_prices(df_yf)
