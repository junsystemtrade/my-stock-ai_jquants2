"""
portfolio_manager.py
====================
株価データの取得・DB 同期を担当するモジュール。

【動作モード】
  sync_data():
    毎日の差分取得。DB最終日の翌日〜今日を取得。
    daily_scan workflow から呼ばれる。

  backfill_data():
    初回バックフィル。DB最古日より前を過去に向かって掘り下げる。
    3年分に満たない場合に Initial Backfill workflow から呼ばれる。
    チェックポイント方式で途中再開可能。

  直接実行（python portfolio_manager.py）:
    backfill_data() を実行する。
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
# 定数
# -----------------------------------------------------------------------
BASE_API          = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"
BACKFILL_YEARS    = 3
_SAMPLE_CODES     = ["72030", "86580", "90840", "30480"]
_JQUANTS_INTERVAL = float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "12.5"))
_BATCH_DAYS       = int(os.getenv("BATCH_DAYS", "5"))
_DB_CHUNK_SIZE    = int(os.getenv("DB_CHUNK_SIZE", "1000"))
_YF_CHUNK_SIZE    = int(os.getenv("YF_CHUNK_SIZE", "100"))
_YF_SLEEP         = float(os.getenv("YF_SLEEP_SEC", "2.0"))


# -----------------------------------------------------------------------
# JST 今日の日付
# -----------------------------------------------------------------------
def _today_jst() -> date:
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


# -----------------------------------------------------------------------
# レートリミッター
# -----------------------------------------------------------------------
class RateLimiter:
    def __init__(self, min_interval_sec: float = _JQUANTS_INTERVAL):
        self.min_interval = min_interval_sec
        self._last        = 0.0

    def wait(self):
        elapsed = time.monotonic() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


# -----------------------------------------------------------------------
# 銘柄コードユーティリティ
# -----------------------------------------------------------------------
def _to_yf_ticker(code: str) -> str:
    code_only = code.replace(".T", "").strip()
    if len(code_only) > 4:
        code_only = code_only[:4]
    return f"{code_only}.T"


def _to_db_ticker(yf_ticker: str) -> str:
    return _to_yf_ticker(yf_ticker)


# -----------------------------------------------------------------------
# JPX 上場銘柄マスタ取得
# -----------------------------------------------------------------------
def get_target_tickers() -> dict:
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
            print("⚠️ JPX銘柄マスタのリンクが見つかりませんでした")
            return {}

        excel_url = base_url + link["href"]
        resp      = requests.get(excel_url, headers=headers, timeout=60)
        df        = pd.read_excel(io.BytesIO(resp.content))
        df.columns = [str(c).strip() for c in df.columns]

        stock_map = {}
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip()
            name = str(row.iloc[2]).strip()
            if len(code) >= 4 and code[:4].isdigit():
                stock_map[f"{code[:4]}.T"] = {"name": name}

        print(f"✅ JPX銘柄マスタ取得完了: {len(stock_map)} 銘柄")
        return stock_map

    except Exception as e:
        print(f"❌ JPX銘柄マスタ取得失敗: {e}")
        return {}


# -----------------------------------------------------------------------
# J-Quants API ヘルパー
# -----------------------------------------------------------------------
def _jq_headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Accept": "application/json"}


def _jq_latest_date(api_key: str, limiter: RateLimiter) -> date | None:
    """J-Quants で取得できる最新日を返す。"""
    url       = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    best_date = None

    for code in _SAMPLE_CODES:
        try:
            limiter.wait()
            r = requests.get(
                url,
                headers=_jq_headers(api_key),
                params={"code": code},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            data = r.json().get("data", [])
            if not data:
                continue
            d = pd.to_datetime(data[-1]["Date"]).date()
            print(f"  📌 code={code}: 最新日 {d}")
            if best_date is None or d > best_date:
                best_date = d
        except Exception as e:
            print(f"  ⚠️ code={code} 取得失敗: {e}")

    return best_date


def _jq_earliest_date(api_key: str, limiter: RateLimiter) -> date | None:
    """J-Quants で取得できる最古日を返す。"""
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"

    for code in _SAMPLE_CODES:
        try:
            limiter.wait()
            r = requests.get(
                url,
                headers=_jq_headers(api_key),
                params={"code": code},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            data = r.json().get("data", [])
            if not data:
                continue
            d = pd.to_datetime(data[0]["Date"]).date()
            print(f"  📌 code={code}: 最古日 {d}")
            return d
        except Exception as e:
            print(f"  ⚠️ code={code} 取得失敗: {e}")

    return None


def _jq_fetch_day(api_key: str, date_str: str, limiter: RateLimiter) -> list[dict]:
    url            = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    rows: list     = []
    pagination_key = None

    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        limiter.wait()
        r = requests.get(
            url,
            headers=_jq_headers(api_key),
            params=params,
            timeout=30,
        )

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "60"))
            print(f"  ⚠️ 429 → {retry_after}秒待機")
            time.sleep(retry_after)
            continue

        if r.status_code in (400, 403, 500, 503):
            return []

        r.raise_for_status()

        payload        = r.json()
        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            break

    return rows


def _jq_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"]   = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = df["Code"].astype(str).apply(lambda c: f"{c[:4]}.T")

    col_map = {"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    required = ["ticker", "date", "open", "high", "low", "price", "volume"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"J-Quants レスポンスに必要カラムなし: {missing}")

    return (
        df[required]
        .drop_duplicates(subset=["ticker", "date"])
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )


# -----------------------------------------------------------------------
# Yahoo Finance フォールバック
# -----------------------------------------------------------------------
def _yf_fetch_range(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()

    all_records = []
    total       = len(tickers)

    for i in range(0, total, _YF_CHUNK_SIZE):
        chunk = tickers[i : i + _YF_CHUNK_SIZE]
        print(f"  📡 Yahoo Finance: {i+1}〜{min(i+_YF_CHUNK_SIZE, total)}/{total} 銘柄")

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
                timeout=30,
            )
        except Exception as e:
            print(f"  ❌ チャンク取得失敗: {e}")
            time.sleep(_YF_SLEEP)
            continue

        if raw is None or raw.empty:
            time.sleep(_YF_SLEEP)
            continue

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
            except (KeyError, TypeError):
                continue

        time.sleep(_YF_SLEEP)

    if not all_records:
        return pd.DataFrame()

    return (
        pd.DataFrame(all_records)
        .drop_duplicates(subset=["ticker", "date"])
        .reset_index(drop=True)
    )


# -----------------------------------------------------------------------
# 日付ユーティリティ
# -----------------------------------------------------------------------
def _business_days(start: date, end: date) -> list[date]:
    days = []
    d    = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


# -----------------------------------------------------------------------
# 共通取得・保存ロジック
# -----------------------------------------------------------------------
def _fetch_and_save(
    target_days: list[date],
    api_key: str,
    jq_latest: date,
    db: database_manager.DBManager,
    limiter: RateLimiter,
    all_yf_tickers: list[str],
    mode: str = "同期",
):
    total_days   = len(target_days)
    saved_total  = 0
    batch_buffer = []
    skipped_days = []

    for idx, target_date in enumerate(target_days, 1):
        d_str    = target_date.strftime("%Y-%m-%d")
        progress = f"[{idx}/{total_days}]"

        print(f"{progress} 📥 {d_str}", end="  ", flush=True)

        if target_date <= jq_latest:
            rows = _jq_fetch_day(api_key, d_str, limiter)
            if rows:
                df = _jq_rows_to_df(rows)
                batch_buffer.append(df)
                print(f"✅ J-Quants {len(df):,}件")
            else:
                print(f"⚠️ J-Quants空 → Yahoo Finance補完中...")
                yf_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
                df_yf  = _yf_fetch_range(all_yf_tickers, d_str, yf_end)
                if not df_yf.empty:
                    batch_buffer.append(df_yf)
                    print(f"  ✅ Yahoo Finance {len(df_yf):,}件")
                else:
                    skipped_days.append(d_str)
                    print(f"  ℹ️ データなし → スキップ")
        else:
            print(f"📡 Yahoo Finance（J-Quants範囲外）", end="  ", flush=True)
            yf_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
            df_yf  = _yf_fetch_range(all_yf_tickers, d_str, yf_end)
            if not df_yf.empty:
                batch_buffer.append(df_yf)
                print(f"✅ {len(df_yf):,}件")
            else:
                skipped_days.append(d_str)
                print(f"ℹ️ データなし → スキップ")

        if len(batch_buffer) >= _BATCH_DAYS:
            combined     = pd.concat(batch_buffer, ignore_index=True)
            db.save_prices(combined)
            saved_total  += len(combined)
            batch_buffer  = []
            print(f"  💾 バッチ保存: 累計 {saved_total:,} 件")

    if batch_buffer:
        combined     = pd.concat(batch_buffer, ignore_index=True)
        db.save_prices(combined)
        saved_total += len(combined)
        print(f"\n💾 最終バッチ保存: {len(combined):,} 件")

    print(f"\n{'='*40}")
    print(f"🎉 {mode} 完了")
    print(f"   保存件数  : {saved_total:,} 件")
    print(f"   取得日数  : {total_days} 営業日")
    if skipped_days:
        print(f"   スキップ  : {len(skipped_days)} 日（祝日等）")
    print(f"{'='*40}")


# -----------------------------------------------------------------------
# 差分同期（毎日の定期実行）
# -----------------------------------------------------------------------
def sync_data():
    """DB の最終日の翌日〜今日を差分取得する。"""
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db      = database_manager.DBManager()
    limiter = RateLimiter()

    print("🔍 J-Quants 取得可能最新日を確認中...")
    jq_latest = _jq_latest_date(api_key, limiter)
    if jq_latest:
        print(f"✅ J-Quants 最新日: {jq_latest}")
    else:
        jq_latest = date(2000, 1, 1)

    today      = _today_jst()
    target_end = today - timedelta(days=1)
    while target_end.weekday() >= 5:
        target_end -= timedelta(days=1)

    db_latest_str = db.get_latest_saved_date()

    if db_latest_str is None:
        start_date = target_end - timedelta(days=7)
        print(f"\n⚠️ DBが空です。直近7日分のみ取得します。")
    else:
        db_latest  = date.fromisoformat(db_latest_str)
        start_date = db_latest + timedelta(days=1)
        if start_date > target_end:
            print(f"✅ DB はすでに最新です（最終日: {db_latest}）。スキップします。")
            return
        print(f"\n🔄 差分取得: {start_date} 〜 {target_end}")

    target_days    = _business_days(start_date, target_end)
    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    _fetch_and_save(target_days, api_key, jq_latest, db, limiter, all_yf_tickers, mode="差分同期")


# -----------------------------------------------------------------------
# バックフィル（過去に向かって掘り下げる）
# -----------------------------------------------------------------------
def backfill_data():
    """
    DBの最古日より前を過去に向かって掘り下げる。
    3年分揃うまで何度でも続きから再開できる。
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db      = database_manager.DBManager()
    limiter = RateLimiter()

    print("🔍 J-Quants 取得可能範囲を確認中...")
    jq_latest   = _jq_latest_date(api_key, limiter)
    jq_earliest = _jq_earliest_date(api_key, limiter)

    if not jq_latest:
        print("❌ J-Quants の最新日を取得できませんでした。終了します。")
        return

    print(f"✅ J-Quants 範囲: {jq_earliest} 〜 {jq_latest}")

    # 目標の開始日（3年前）
    target_start = jq_latest - timedelta(days=BACKFILL_YEARS * 365)
    print(f"🎯 目標開始日: {target_start}（{BACKFILL_YEARS}年前）")

    # DB の最古日を確認
    db_oldest_str = db.get_oldest_saved_date()

    if db_oldest_str is None:
        fetch_end   = jq_latest
        fetch_start = target_start
        print(f"\n🆕 初回バックフィル: {fetch_start} 〜 {fetch_end}")
    else:
        db_oldest = date.fromisoformat(db_oldest_str)
        if db_oldest <= target_start:
            print(f"✅ 3年分のデータがすでに揃っています（最古日: {db_oldest}）。")
            return
        fetch_end   = db_oldest - timedelta(days=1)
        fetch_start = target_start
        print(f"\n🔄 バックフィル続き: {fetch_start} 〜 {fetch_end}（DB最古日: {db_oldest}）")

    target_days = _business_days(fetch_start, fetch_end)
    total_days  = len(target_days)

    if total_days == 0:
        print("✅ 取得対象の営業日がありません。")
        return

    print(f"📅 取得対象: {total_days} 営業日")
    print(f"⏱ 途中で止まっても次回実行時に続きから再開します\n")

    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    _fetch_and_save(target_days, api_key, jq_latest, db, limiter, all_yf_tickers, mode="バックフィル")


# -----------------------------------------------------------------------
# 直接実行時はバックフィルモード
# -----------------------------------------------------------------------
if __name__ == "__main__":
    backfill_data()
