import os
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime
import database_manager

def get_listed_info(headers):
    """J-Quants V2 APIから上場銘柄一覧を取得"""
    print("🔍 J-Quants V2 から上場銘柄リストを取得中...")
    url = "https://jpx-jquants.com/api/v2/listed/info"
    try:
        res = requests.get(url, headers=headers, timeout=20)
        res.raise_for_status()
        data = res.json().get("info", [])
        return pd.DataFrame(data)
    except Exception as e:
        print(f"❌ 銘柄リスト取得失敗: {e}")
        return pd.DataFrame()

def sync_data(force_initial=False):
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # 1. 銘柄名簿を取得
    df_info = get_listed_info(headers)
    if df_info.empty: return

    # 村田さんの戦略に合わせて、まずは上位100銘柄や特定市場（例: Prime）に絞ることも可能
    # ここではテストとして全銘柄のループを想定（実際は一括取得が効率的）
    tickers = df_info['Code'].unique() 
    period = "5y" if force_initial else "5d"
    
    print(f"🚀 同期開始: {len(tickers)} 銘柄 / 期間: {period}")

    for code in tickers:
        # codeは "30480" 形式なので、yfinance用に "3048.T" へ変換
        yf_ticker = f"{code[:4]}.T"
        print(f"🔄 同期中: {yf_ticker}")

        # --- A. yfinance で広域取得 (過去5年分をカバー) ---
        try:
            df_yf = yf.download(yf_ticker, period=period, interval="1d", progress=False, auto_adjust=True)
            if not df_yf.empty:
                if isinstance(df_yf.columns, pd.MultiIndex):
                    df_yf.columns = df_yf.columns.get_level_values(0)
                
                df_yf['ticker'] = code[:4]
                df_yf['date'] = df_yf.index.date
                df_yf = df_yf.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"
                })
                
                # --- B. J-Quants V2 で直近の正確な値を上書き ---
                jq_url = f"https://jpx-jquants.com/api/v2/prices/daily?code={code}"
                res_jq = requests.get(jq_url, headers=headers, timeout=10)
                if res_jq.status_code == 200:
                    jq_quotes = res_jq.json().get("daily_quotes", [])
                    if jq_quotes:
                        df_jq = pd.DataFrame(jq_quotes)
                        df_jq['date'] = pd.to_datetime(df_jq['Date']).dt.date
                        df_jq['ticker'] = code[:4]
                        df_jq = df_jq.rename(columns={
                            "Open": "open", "High": "high", "Low": "low", "Close": "price", "Volume": "volume"
                        })
                        
                        # yfinanceのデータと統合（J-Quants優先）
                        df_yf = pd.concat([df_yf, df_jq[['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']]])
                        df_yf = df_yf.drop_duplicates(subset=['date', 'ticker'], keep='last')

                # DB保存
                db.save_prices(df_yf)
        except Exception as e:
            print(f"⚠️ {yf_ticker} 同期エラー: {e}")

if __name__ == "__main__":
    sync_data(force_initial=False)
