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
WEQ_DAILY_ENDPOINT = "/equities/bars/daily"
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
    try:
        raw = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,
            progress=False,
            timeout=30,
        )
    except Exception:
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    records = []
    for idx, row in raw.iterrows():
        close = row.get("Close")
        if pd.isna(close):
            continue
        records.append({
            "ticker": _to_db_ticker(ticker),
            "date":   idx.date(),
            "open":   float(row["Open"])   if pd.notna(row.get("Open"))   else None,
            "high":   float(row["High"])   if pd.notna(row.get("High"))   else None,
            "low":    float(row["Low"])    if pd.notna(row.get("Low"))    else None,
            "price":  float(close),
            "volume": int(row["Volume"])   if pd.notna(row.get("Volume")) else None,
        })

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def _yf_fetch_chunk(tickers, start, end):
    if not tickers:
        return pd.DataFrame()

    all_records = []
    n_chunks    = (len(tickers) + _YF_CHUNK_SIZE - 1) // _YF_CHUNK_SIZE

    for i in range(0, len(tickers), _YF_CHUNK_SIZE):
        chunk    = tickers[i : i + _YF_CHUNK_SIZE]
        chunk_no = i // _YF_CHUNK_SIZE + 1
        print(f"  chunk {chunk_no}/{n_chunks} ({len(chunk)} tickers)", end=" ... ", flush=True)

        try:
            raw = yf.download(
                chunk,
                start=start,
                end=end,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
                timeout=60,
            )
        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(_YF_SLEEP * 2)
            continue

        if raw is None or raw.empty:
            print("empty")
            time.sleep(_YF_SLEEP)
            continue

        count = 0
        for ticker in chunk:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    df = raw[ticker].dropna(how="all")
                else:
                    df = raw.dropna(how="all")

                for idx, row in df.iterrows():
                    close = row.get("Close")
                    if pd.isna(close):
                        continue
                    all_records.append({
                        "ticker": _to_db_ticker(ticker),
                        "date":   idx.date(),
                        "open":   float(row["Open"])   if pd.notna(row.get("Open"))   else None,
                        "high":   float(row["High"])   if pd.notna(row.get("High"))   else None,
                        "low":    float(row["Low"])    if pd.notna(row.get("Low"))    else None,
                        "price":  float(close),
                        "volume": int(row["Volume"])   if pd.notna(row.get("Volume")) else None,
                    })
                    count += 1
            except (KeyError, TypeError):
                continue

        print(f"OK {count} rows")
        time.sleep(_YF_SLEEP)

    if not all_records:
        return pd.DataFrame()

    return (
        pd.DataFrame(all_records)
        .drop_duplicates(subset=["ticker", "date"])
        .reset_index(drop=True)
    )


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

    print("Checking JQuants latest date...")
    jq_latest = _jq_latest_date()
    if jq_latest:
        print(f"JQuants latest: {jq_latest}")

    target_end    = _prev_business_day(_today_jst())
    db_latest_str = db.get_latest_saved_date()

    if db_latest_str is None:
        print("WARNING: No data in DB. Run Initial Backfill first.")
        return

    db_latest  = date.fromisoformat(db_latest_str)
    start_date = db_latest + timedelta(days=1)

    if start_date > target_end:
        print(f"OK DB is up to date (latest: {db_latest}). Skip.")
        return

    print(f"Sync: {start_date} to {target_end}")

    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())
    start_str      = start_date.strftime("%Y-%m-%d")
    end_str        = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Yahoo Finance bulk fetch: {len(all_yf_tickers)} tickers")
    df = _yf_fetch_chunk(all_yf_tickers, start_str, end_str)

    if df.empty:
        print("WARNING: No data fetched.")
        return

    db.save_prices(df)
    print(f"DONE sync: {len(df)} rows saved")


def backfill_data():
    db = database_manager.DBManager()
    today = _today_jst()
    target_start = today - timedelta(days=BACKFILL_YEARS * 365)
    target_end = _prev_business_day(today)
    start_str = target_start.strftime("%Y-%m-%d")
    end_str = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

    ticker_map = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())
    total = len(all_yf_tickers)

    existing_tickers = _get_existing_tickers(db, target_start)
    remaining = [t for t in all_yf_tickers if t not in existing_tickers]

    print(f"Backfill: {total} tickers / remaining: {len(remaining)}")
    print(f"Period: {start_str} to {end_str}")

    chunk_size = 50
    n_chunks = (len(remaining) + chunk_size - 1) // chunk_size
    saved_total = 0

    for i in range(0, len(remaining), chunk_size):
        chunk = remaining[i: i + chunk_size]
        chunk_no = i // chunk_size + 1
        print(f"[{chunk_no}/{n_chunks}] {len(chunk)} tickers", end=" ... ", flush=True)

        try:
            raw = yf.download(
                chunk,
                start=start_str,
                end=end_str,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
                timeout=60,
            )
        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(5)
            continue

        if raw is None or raw.empty:
            print("empty")
            time.sleep(3)
            continue

        records = []
        for ticker in chunk:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    df = raw[ticker].dropna(how="all")
                else:
                    df = raw.dropna(how="all")
                for idx, row in df.iterrows():
                    close = row.get("Close")
                    if pd.isna(close):
                        continue
                    records.append({
                        "ticker": _to_db_ticker(ticker),
                        "date": idx.date(),
                        "open": float(row["Open"]) if pd.notna(row.get("Open")) else None,
                        "high": float(row["High"]) if pd.notna(row.get("High")) else None,
                        "low": float(row["Low"]) if pd.notna(row.get("Low")) else None,
                        "price": float(close),
                        "volume": int(row["Volume"]) if pd.notna(row.get("Volume")) else None,
                    })
            except (KeyError, TypeError):
                continue

        if records:
            df_out = pd.DataFrame(records).drop_duplicates(subset=["ticker", "date"])
            db.save_prices(df_out)
            saved_total += len(df_out)
            print(f"OK {len(df_out)} rows (total: {saved_total})")
        else:
            print("no data")

        time.sleep(3)

    total_rows = _count_rows(db)
    print(f"Backfill complete: {saved_total} rows saved / DB total: {total_rows}")
    if total_rows > 2000000:
        print("SUCCESS: enough data collected!")
    else:
        print("INCOMPLETE: run again to continue.")

if __name__ == "__main__":
    backfill_data()
c
