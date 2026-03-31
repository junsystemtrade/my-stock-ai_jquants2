"""
portfolio_manager.py
====================
株価データの取得・DB同期を担当。
【一括モード】2年分のデータを強制的にバックフィルします。
"""

import io
import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
import database_manager

# -----------------------------------------------------------------------
# 定数
# -----------------------------------------------------------------------
BASE_API          = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"
BACKFILL_YEARS    = 2          # ターゲット期間
SAMPLE_CODE       = "30480"    # ビックカメラ

class RateLimiter:
    def __init__(self, min_interval_sec: float = 13.0):
        self.min_interval = float(min_interval_sec)
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()

def get_target_tickers() -> dict:
    from bs4 import BeautifulSoup
    base_url  = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers   = {"User-Agent": "Mozilla/5.0"}
    try:
        res  = requests.get(list_page, headers=headers, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)
        if not link: return {}
        excel_url = base_url + link["href"]
        resp      = requests.get(excel_url, headers=headers, timeout=60)
        df        = pd.read_excel(io.BytesIO(resp.content))
        stock_map = {}
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip()
            name = str(row.iloc[2]).strip()
            if len(code) == 4 and code.isdigit():
                stock_map[f"{code}.T"] = {"name": name}
        return stock_map
    except Exception as e:
        print(f"❌ JPX銘柄マスタ取得失敗: {e}")
        return {"30480.T": {"name": "ビックカメラ"}}

def _jquants_headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Accept": "application/json"}

def _jquants_latest_accessible_date(api_key: str, limiter: RateLimiter) -> date:
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    limiter.wait()
    r = requests.get(url, headers=_jquants_headers(api_key), params={"code": SAMPLE_CODE}, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    return pd.to_datetime(data[-1]["Date"]).date()

def _jquants_fetch_day(api_key: str, date_str: str, limiter: RateLimiter) -> list[dict]:
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    rows = []
    pagination_key = None
    while True:
        params = {"date": date_str}
        if pagination_key: params["pagination_key"] = pagination_key
        limiter.wait()
        r = requests.get(url, headers=_jquants_headers(api_key), params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(60); continue
        if r.status_code == 400: return []
        r.raise_for_status()
        payload = r.json()
        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")
        if not pagination_key: break
    return rows

def _jquants_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = df["Code"].astype(str)
    col_map = {"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df[["ticker", "date", "open", "high", "low", "price", "volume"]].reset_index(drop=True)

def _yahoo_fetch_range(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    if not tickers: return pd.DataFrame()
    print(f"📡 Yahoo Finance 取得中... ({start})")
    try:
        raw = yf.download(tickers, start=start, end=end, interval="1d", auto_adjust=True, progress=False, group_by="ticker")
        records = []
        for ticker in tickers:
            try:
                df_t = raw[ticker].dropna(how="all")
                for idx, row in df_t.iterrows():
                    records.append({
                        "ticker": ticker.replace(".T", ""), "date": idx.date(),
                        "open": row.get("Open"), "high": row.get("High"),
                        "low": row.get("Low"), "price": row.get("Close"),
                        "volume": row.get("Volume"),
                    })
            except: continue
        return pd.DataFrame(records)
    except Exception as e:
        print(f"❌ Yahoo Finance 失敗: {e}")
        return pd.DataFrame()

def _business_days(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5: days.append(d)
        d += timedelta(days=1)
    return days

# -----------------------------------------------------------------------
# メイン同期関数（強制バックフィル仕様）
# -----------------------------------------------------------------------
def sync_data():
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    db = database_manager.DBManager()
    limiter = RateLimiter(min_interval_sec=float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "13")))

    print("🔍 データ同期プロセス開始...")
    jquants_latest = _jquants_latest_accessible_date(api_key, limiter)
    
    # 【重要】開始日を「今日から2年前」に固定して、全日程をチェックする
    start_date = jquants_latest - timedelta(days=BACKFILL_YEARS * 365)
    print(f"🚀 【強制モード】2年分バックフィル実行中: {start_date} 〜 {jquants_latest}")

    target_days = _business_days(start_date, jquants_latest)
    print(f"📅 合計チェック日数: {len(target_days)} 日")

    ticker_map = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    for target_date in target_days:
        d_str = target_date.strftime("%Y-%m-%d")
        
        # まず J-Quants を試す
        rows = _jquants_fetch_day(api_key, d_str, limiter)
        
        if rows:
            df = _jquants_rows_to_df(rows)
            db.save_prices(df)
            print(f"✅ {d_str}: J-Quants {len(df)} 件保存")
        else:
            # 12週間より前などは Yahoo Finance で補完
            yf_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
            df_yf = _yahoo_fetch_range(all_yf_tickers, d_str, yf_end)
            if not df_yf.empty:
                db.save_prices(df_yf)
                print(f"✅ {d_str}: Yahoo Finance {len(df_yf)} 件保存")
            
            # Yahooへの連続アクセス負荷を考慮して少し待機
            time.sleep(3)

    print(f"\n🎉 同期完了！DBを確認してください。")

if __name__ == "__main__":
    sync_data()
