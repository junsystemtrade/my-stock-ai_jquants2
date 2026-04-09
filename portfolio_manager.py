"""
portfolio_manager.py
====================
株価データの取得（yfinance）および DB への保存を担当。
"""

import io
import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

import database_manager

# -----------------------------------------------------------------------
# 設定定数
# -----------------------------------------------------------------------
BACKFILL_YEARS = 3
_YF_SLEEP = float(os.getenv("YF_SLEEP_SEC", "3.0"))
MARKET_TICKER = "NIY=F"  # 日経平均先物(CME)

def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()

def _to_yf_ticker(code):
    if "=" in code or "^" in code:
        return code
    code_only = str(code).replace(".T", "").strip()
    return f"{code_only[:4]}.T" if len(code_only) >= 4 else code_only

# -----------------------------------------------------------------------
# 銘柄マスター取得
# -----------------------------------------------------------------------
def get_target_tickers():
    """JPXから最新の銘柄リストを取得。失敗時はサンプルを返す。"""
    base_url = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(list_page, headers=headers, timeout=20)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)
        
        if link:
            excel_url = base_url + link["href"]
            resp = requests.get(excel_url, headers=headers, timeout=30)
            df = pd.read_excel(io.BytesIO(resp.content))
            
            stock_map = {}
            for _, row in df.iterrows():
                code = str(row.iloc[1]).strip()
                name = str(row.iloc[2]).strip()
                if code.isdigit() and len(code) >= 4:
                    stock_map[f"{code[:4]}.T"] = {"name": name}
            
            print(f"✅ JPXマスター取得完了: {len(stock_map)} 銘柄")
            return stock_map
    except Exception as e:
        print(f"⚠️ JPXマスター取得エラー (サンプルを使用します): {e}")
    
    return {"7203.T": {"name": "トヨタ"}, "8306.T": {"name": "三菱UFJ"}}

# -----------------------------------------------------------------------
# データダウンロード
# -----------------------------------------------------------------------
def _yf_fetch_chunk(tickers, start, end):
    """複数銘柄を一括ダウンロードして整形"""
    if not tickers:
        return pd.DataFrame()
    
    print(f" 📥 Download {len(tickers)} tickers...", end="", flush=True)
    try:
        raw = yf.download(tickers, start=start, end=end, interval="1d", 
                          auto_adjust=True, progress=False, group_by="ticker", threads=True)
    except Exception as e:
        print(f" ❌ Error: {e}")
        return pd.DataFrame()
    
    if raw.empty:
        print(" ⚠️ No Data")
        return pd.DataFrame()

    all_dfs = []
    # yfinanceの戻り値が単一銘柄か複数銘柄かで処理を分岐
    fetched_tickers = tickers if len(tickers) == 1 else raw.columns.levels[0]

    for t in fetched_tickers:
        try:
            df_t = raw[t].copy() if len(tickers) > 1 else raw.copy()
            if df_t.empty or "Close" not in df_t.columns:
                continue
                
            df_t["ticker"] = t
            df_t["date"] = df_t.index.date
            df_t["price"] = df_t["Close"]
            df_t = df_t.rename(columns={"Open": "open", "High": "high", "Low": "low", "Volume": "volume"})
            
            # 必要なカラムに絞り込み、欠損値を処理
            valid_df = df_t[["ticker", "date", "open", "high", "low", "price", "volume"]].dropna(subset=["price"])
            if not valid_df.empty:
                all_dfs.append(valid_df)
        except:
            continue

    print(" Done")
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

# -----------------------------------------------------------------------
# 同期・バックフィル
# -----------------------------------------------------------------------
def _prev_business_day(d):
    d -= timedelta(days=1)
    while d.weekday() >= 5: d -= timedelta(days=1)
    return d

def sync_data():
    """日次の差分更新"""
    db = database_manager.DBManager()
    target_end = _prev_business_day(_today_jst())
    
    latest_date_str = db.get_latest_saved_date()
    if not latest_date_str:
        print("⚠️ DBにデータがありません。backfillを先に実行してください。")
        return

    start_date = date.fromisoformat(latest_date_str) + timedelta(days=1)
    if start_date > target_end:
        print(f"✅ DBは最新です (最新日: {latest_date_str})。スキップします。")
        return

    # 銘柄リスト準備（市場地合いを必ず含める）
    tickers = list(get_target_tickers().keys())
    if MARKET_TICKER not in tickers:
        tickers.append(MARKET_TICKER)

    print(f"🔄 同期開始: {start_date} -> {target_end}")
    df = _yf_fetch_chunk(tickers, start_date.strftime("%Y-%m-%d"), 
                         (target_end + timedelta(days=1)).strftime("%Y-%m-%d"))
    
    if not df.empty:
        db.save_prices(df)
        print(f"✨ 同期完了: {len(df)} 件のデータを保存")

def backfill_data():
    """過去数年分のデータを一括取得（初回用）"""
    db = database_manager.DBManager()
    today = _today_jst()
    start_str = (today - timedelta(days=BACKFILL_YEARS * 365)).strftime("%Y-%m-%d")
    end_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    tickers = list(get_target_tickers().keys())
    if MARKET_TICKER not in tickers:
        tickers.append(MARKET_TICKER)

    # すでにDBにある銘柄を除外
    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            existing = {row[0] for row in conn.execute(text("SELECT ticker FROM daily_prices GROUP BY ticker"))}
    except:
        existing = set()

    remaining = [t for t in tickers if t not in existing]
    print(f"🚀 バックフィル開始: 残り {len(remaining)} 銘柄")

    chunk_size = 50
    for i in range(0, len(remaining), chunk_size):
        chunk = remaining[i : i + chunk_size]
        print(f"\n--- Batch {i//chunk_size + 1} / {(len(remaining)-1)//chunk_size + 1} ---")
        df_chunk = _yf_fetch_chunk(chunk, start_str, end_str)
        
        if not df_chunk.empty:
            db.save_prices(df_chunk)
        
        time.sleep(_YF_SLEEP)

    print(f"\n✨ 全バックフィル工程が正常に終了しました。")

if __name__ == "__main__":
    backfill_data()
