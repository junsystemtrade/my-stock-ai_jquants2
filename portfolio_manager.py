import io
import os
import time
import requests
import pandas as pd
import yfinance as yf

from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

import database_manager

BACKFILL_YEARS    = 3
_YF_SINGLE_SLEEP  = float(os.getenv("YF_SINGLE_SLEEP", "0.1"))
_YF_CHUNK_SIZE    = int(os.getenv("YF_CHUNK_SIZE", "50"))
_YF_SLEEP         = float(os.getenv("YF_SLEEP_SEC", "3.0"))
_DB_CHUNK_SIZE    = int(os.getenv("DB_CHUNK_SIZE", "1000"))
BASE_API          = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"
_SAMPLE_CODES     = ["72030", "86580", "90840", "30480"]
_JQUANTS_INTERVAL = float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "12.5"))


def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


def _to_yf_ticker(code):
    code_only = code.replace(".T", "").strip()
    if len(code_only) > 4:
        code_only = code_only[:4]
    return f"{code_only}.T"


def _to_db_ticker(yf_ticker):
    return _to_yf_ticker(yf_ticker)


def get_target_tickers():
    try:
        import jpx_master
        raw = jpx_master.get_target_tickers()
        return {_to_yf_ticker(k): v for k, v in raw.items()}
    except ImportError:
        pass

    from bs4 import BeautifulSoup

    base_url  = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers   = {"User-Agent": "Mozilla/5.0"}

    try:
        res  = requests.get(list_page, headers=headers, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)
        if not link:
            print("WARNING: JPX master link not found")
            return {}

        excel_url = base_url + link["href"]
        resp      = requests.get(excel_url, headers=headers, timeout=60)

        if excel_url.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
        else:
            df = pd.read_excel(io.BytesIO(resp.content), engine="xlrd")

        df.columns = [str(c).strip() for c in df.columns]

        stock_map = {}
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip()
            name = str(row.iloc[2]).strip()
            if len(code) >= 4 and code[:4].isdigit():
                stock_map[f"{code[:4]}.T"] = {"name": name}

        print(f"OK JPX master: {len(stock_map)} tickers")
        return stock_map

    except Exception as e:
        print(f"ERROR JPX master: {e}")
        return {}


def _jq_latest_date():
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        return None

    url      = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    best     = None
    last_req = 0.0

    for code in _SAMPLE_CODES:
        try:
            elapsed = time.monotonic() - last_req
            if elapsed < _JQUANTS_INTERVAL:
                time.sleep(_JQUANTS_INTERVAL - elapsed)
            last_req = time.monotonic()

            r = requests.get(
                url,
                headers={"x-api-key": api_key, "Accept": "application/json"},
                params={"code": code},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            data = r.json().get("data", [])
            if not data:
                continue
            d = pd.to_datetime(data[-1]["Date"]).date()
            print(f"  JQuants code={code}: latest={d}")
            if best is None or d > best:
                best = d
        except Exception:
            continue

    return best


def _yf_fetch_single(ticker, start, end):
    """Fallback用: 1銘柄だけ取得する場合もベクトル演算で高速化"""
    try:
        raw = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=True, progress=False, timeout=30)
    except:
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    # 1行ずつのループ(iterrows)を廃止し、ベクトル演算で処理
    df = raw.copy()
    df["ticker"] = _to_db_ticker(ticker)
    df["date"]   = df.index.date
    df["price"]  = df["Close"]
    
    # 必要列のリネームと抽出
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Volume": "volume"})
    final_cols = ["ticker", "date", "open", "high", "low", "price", "volume"]
    return df[final_cols].dropna(subset=["price"]).reset_index(drop=True)


def _yf_fetch_chunk(tickers, start, end):
    if not tickers:
        return pd.DataFrame()

    try:
        raw = yf.download(
            tickers,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
            timeout=120,
        )
    except Exception as e:
        print(f" ERROR: {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    all_dfs = []
    # yfinanceは複数銘柄だとMultiIndexになる
    if isinstance(raw.columns, pd.MultiIndex):
        fetched_tickers = raw.columns.levels[0]
    else:
        # 1銘柄しか取れなかった場合
        return _yf_fetch_single(tickers[0], start, end)

    for t in fetched_tickers:
        if t not in raw: continue
        df_t = raw[t].dropna(how="all").copy()
        if df_t.empty: continue
        
        # ベクトル演算で一括変換
        df_t["ticker"] = _to_db_ticker(t)
        df_t["date"]   = df_t.index.date
        df_t["price"]  = df_t["Close"]
        df_t = df_t.rename(columns={"Open": "open", "High": "high", "Low": "low", "Volume": "volume"})
        
        final_cols = ["ticker", "date", "open", "high", "low", "price", "volume"]
        all_dfs.append(df_t[final_cols].dropna(subset=["price"]))

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def _prev_business_day(d):
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _get_existing_tickers(db, since):
    from sqlalchemy import text
    query = text("""
        SELECT ticker
        FROM daily_prices
        WHERE date >= :since
        GROUP BY ticker
        HAVING COUNT(*) >= 700
    """)
    try:
        with db.engine.connect() as conn:
            result = conn.execute(query, {"since": str(since)})
            return {row[0] for row in result}
    except Exception:
        return set()


def _count_rows(db):
    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM daily_prices"))
            return result.fetchone()[0]
    except Exception:
        return 0


def sync_data():
    db = database_manager.DBManager()
    target_end = _prev_business_day(_today_jst())
    db_latest_str = db.get_latest_saved_date()

    if db_latest_str is None:
        print("WARNING: No data in DB. Run Initial Backfill first.")
        return

    db_latest  = date.fromisoformat(db_latest_str)
    start_date = db_latest + timedelta(days=1)

    if start_date > target_end:
        print(f"OK DB is up to date (latest: {db_latest}). Skip.")
        return

    ticker_map = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Sync: {start_str} to {target_end}")
    df = _yf_fetch_chunk(all_yf_tickers, start_str, end_str)

    if not df.empty:
        db.save_prices(df)
        print(f"DONE sync: {len(df)} rows saved")


def backfill_data():
    """一括取得(Bulk Mode)に最適化したバックフィル"""
    db = database_manager.DBManager()
    today = _today_jst()
    target_start = today - timedelta(days=BACKFILL_YEARS * 365)
    target_end = _prev_business_day(today)
    start_str = target_start.strftime("%Y-%m-%d")
    end_str = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

    ticker_map = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())
    
    existing_tickers = _get_existing_tickers(db, target_start)
    remaining = [t for t in all_yf_tickers if t not in existing_tickers]

    print(f"Backfill (FAST MODE): Total {len(all_yf_tickers)} / Remaining {len(remaining)}")
    
    chunk_size = 50 
    saved_total = 0

    for i in range(0, len(remaining), chunk_size):
        chunk = remaining[i : i + chunk_size]
        current_count = i + len(chunk)
        print(f" [{current_count}/{len(remaining)}] Downloading {len(chunk)} tickers...", end="", flush=True)
        
        df_chunk = _yf_fetch_chunk(chunk, start_str, end_str)
        
        if not df_chunk.empty:
            db.save_prices(df_chunk)
            saved_total += len(df_chunk)
            print(f" OK (+{len(df_chunk)} rows)")
        else:
            print(" SKIP (no data)")
        
        # ブロック防止のための適度なスリープ
        time.sleep(_YF_SLEEP)

    total_rows = _count_rows(db)
    print(f"\nBackfill complete: Saved {saved_total} rows / DB Total: {total_rows}")

if __name__ == "__main__":
    backfill_data()
