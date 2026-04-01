"""
portfolio_manager.py
====================
株価データの取得・DB 同期を担当するモジュール。

【高速化の方針】
  Yahoo Finance を主軸に据えることで大幅に高速化する。

  J-Quants Free プランは 5 req/min のレート制限があり、
  全銘柄 × 3年分を取得すると 6〜8 時間かかる。

  Yahoo Finance は 1 リクエストで複数銘柄 × 複数年分を
  一括取得できるため、同じデータを 30〜60 分で取得可能。

【動作モード】
  sync_data():
    毎日の差分取得。daily_scan.yml から呼ばれる。
    DB最終日の翌日〜今日を Yahoo Finance で高速取得。

  backfill_data():
    Initial Backfill workflow から呼ばれる。
    DB最古日より前を Yahoo Finance で一括高速取得。
    3年分を 30〜60 分で完了させる。

【所要時間の目安】
  Yahoo Finance: 100銘柄/チャンク × 4000銘柄 = 40チャンク × 3秒 = 約2分/日
  ただし3年分をまとめて取るので: 40チャンク × 3秒 = 約2分で3年分完了
  → 合計 30〜60 分（J-Quants の 1/10 以下）
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
BACKFILL_YEARS = 3

# Yahoo Finance チャンクサイズ（1リクエストあたりの銘柄数）
# 大きすぎるとタイムアウトするので 50 が安定
_YF_CHUNK_SIZE = int(os.getenv("YF_CHUNK_SIZE", "50"))

# Yahoo Finance チャンク間の待機秒数
_YF_SLEEP = float(os.getenv("YF_SLEEP_SEC", "3.0"))

# DB INSERT の分割単位（Supabase タイムアウト対策）
_DB_CHUNK_SIZE = int(os.getenv("DB_CHUNK_SIZE", "1000"))

# J-Quants（最新日確認のみに使用）
BASE_API          = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"
_SAMPLE_CODES     = ["72030", "86580", "90840", "30480"]
_JQUANTS_INTERVAL = float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "12.5"))


# -----------------------------------------------------------------------
# JST 今日の日付
# -----------------------------------------------------------------------
def _today_jst() -> date:
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


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
    """
    JPX の上場銘柄一覧を取得し {yf_ticker: {"name": str}} を返す。
    ticker は Yahoo Finance 形式（4桁.T）に統一。
    """
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

        print(f"✅ JPX銘柄マスタ取得完了: {len(stock_map)} 銘柄")
        return stock_map

    except Exception as e:
        print(f"❌ JPX銘柄マスタ取得失敗: {e}")
        return {}


# -----------------------------------------------------------------------
# J-Quants: 最新日確認のみ（レート制限対応）
# -----------------------------------------------------------------------
class _RateLimiter:
    def __init__(self, min_interval_sec: float = _JQUANTS_INTERVAL):
        self.min_interval = min_interval_sec
        self._last        = 0.0

    def wait(self):
        elapsed = time.monotonic() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


def _jq_latest_date() -> date | None:
    """
    J-Quants から取得可能な最新日を確認する。
    データ取得には使わず、日付の確認のみに利用。
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        return None

    url     = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    limiter = _RateLimiter()
    best    = None

    for code in _SAMPLE_CODES:
        try:
            limiter.wait()
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
            print(f"  📌 J-Quants code={code}: 最新日 {d}")
            if best is None or d > best:
                best = d
        except Exception:
            continue

    return best


# -----------------------------------------------------------------------
# Yahoo Finance 一括取得（メインのデータソース）
# -----------------------------------------------------------------------
def _yf_fetch_bulk(
    tickers: list[str],
    start: str,
    end: str,
    label: str = "",
) -> pd.DataFrame:
    """
    Yahoo Finance から start〜end の全銘柄データを一括取得。

    【高速化のポイント】
    - 1リクエストで複数銘柄・複数年分を取得（J-Quantsと違い日付ループ不要）
    - _YF_CHUNK_SIZE 銘柄ずつ分割してタイムアウトを防ぐ
    - threads=True で内部並列取得

    tickers : ["3048.T", ...] 形式
    start   : "YYYY-MM-DD"
    end     : "YYYY-MM-DD"（取得したい最終日の翌日）
    """
    if not tickers:
        return pd.DataFrame()

    all_records = []
    total       = len(tickers)
    n_chunks    = (total + _YF_CHUNK_SIZE - 1) // _YF_CHUNK_SIZE

    print(f"📡 Yahoo Finance 一括取得: {total} 銘柄 / {start}〜{end} / {n_chunks}チャンク")

    for i in range(0, total, _YF_CHUNK_SIZE):
        chunk    = tickers[i : i + _YF_CHUNK_SIZE]
        chunk_no = i // _YF_CHUNK_SIZE + 1
        print(f"  チャンク {chunk_no}/{n_chunks} ({len(chunk)}銘柄)", end=" ... ", flush=True)

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
            print(f"❌ 失敗: {e}")
            time.sleep(_YF_SLEEP * 2)
            continue

        if raw is None or raw.empty:
            print("空データ")
            time.sleep(_YF_SLEEP)
            continue

        chunk_records = 0
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
                    chunk_records += 1
            except (KeyError, TypeError):
                continue

        print(f"✅ {chunk_records:,}件")
        time.sleep(_YF_SLEEP)

    if not all_records:
        return pd.DataFrame()

    df_out = (
        pd.DataFrame(all_records)
        .drop_duplicates(subset=["ticker", "date"])
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    print(f"  合計: {len(df_out):,} 件取得")
    return df_out


# -----------------------------------------------------------------------
# 日付ユーティリティ
# -----------------------------------------------------------------------
def _prev_business_day(d: date) -> date:
    """指定日の前営業日を返す。"""
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


# -----------------------------------------------------------------------
# 通常差分同期（daily_scan.yml から呼ばれる）
# -----------------------------------------------------------------------
def sync_data():
    """
    DB の最終保存日の翌日〜今日を差分取得して保存する。
    Yahoo Finance で高速一括取得。
    """
    db = database_manager.DBManager()

    # J-Quants 最新日確認（ログ用）
    print("🔍 J-Quants 最新日を確認中（参考情報）...")
    jq_latest = _jq_latest_date()
    if jq_latest:
        print(f"✅ J-Quants 最新日: {jq_latest}")

    # 取得すべき最終日（前営業日）
    target_end = _prev_business_day(_today_jst())

    # DB 最終日確認
    db_latest_str = db.get_latest_saved_date()
    if db_latest_str is None:
        print("⚠️ DBにデータがありません。Initial Backfill を先に実行してください。")
        return

    db_latest  = date.fromisoformat(db_latest_str)
    start_date = db_latest + timedelta(days=1)

    if start_date > target_end:
        print(f"✅ DB はすでに最新です（最終日: {db_latest}）。スキップします。")
        return

    diff_days = (target_end - db_latest).days
    print(f"🔄 差分取得: {start_date} 〜 {target_end}（{diff_days}日分）")

    # Yahoo Finance で一括取得
    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

    df = _yf_fetch_bulk(all_yf_tickers, start_str, end_str, label="差分")

    if df.empty:
        print("⚠️ データが取得できませんでした。")
        return

    db.save_prices(df)

    print(f"\n{'='*40}")
    print(f"🎉 差分同期完了: {len(df):,} 件保存")
    print(f"{'='*40}")


# -----------------------------------------------------------------------
# バックフィル専用（Initial Backfill workflow から呼ばれる）
# -----------------------------------------------------------------------
def backfill_data():
    """
    DB 最古日より前を過去に向かって一括取得する。
    Yahoo Finance を使うことで J-Quants の 1/10 以下の時間で完了。

    【所要時間目安】
    4000銘柄 / 50銘柄チャンク = 80チャンク × 3秒 = 約4分
    ただし Yahoo Finance のレート制限により実際は 30〜60 分程度
    """
    db = database_manager.DBManager()

    # バックフィルの目標開始日（3年前）
    today        = _today_jst()
    target_start = today - timedelta(days=BACKFILL_YEARS * 365)
    target_end   = _prev_business_day(today)

    # DB 状況確認
    db_oldest_str = db.get_oldest_saved_date()
    db_latest_str = db.get_latest_saved_date()

    print(f"\n📊 DB 現在の状況:")
    print(f"   最古日: {db_oldest_str or 'なし'}")
    print(f"   最終日: {db_latest_str or 'なし'}")
    print(f"   目標開始日: {target_start}（{BACKFILL_YEARS}年前）")

    # --- 過去方向のバックフィル ---
    if db_oldest_str is not None:
        db_oldest     = date.fromisoformat(db_oldest_str)
        backfill_end  = db_oldest - timedelta(days=1)

        if db_oldest <= target_start:
            print(f"\n✅ 過去方向: すでに {BACKFILL_YEARS} 年分揃っています。")
        else:
            print(f"\n⬅️  過去方向バックフィル: {target_start} 〜 {backfill_end}")
            _run_bulk_fetch(db, target_start, backfill_end, label="過去")

    else:
        # DB が空の場合は全期間取得
        print(f"\n⬅️  初回バックフィル: {target_start} 〜 {target_end}")
        _run_bulk_fetch(db, target_start, target_end, label="初回")
        return

    # --- 未来方向の差分補完（J-Quants 範囲外 = 2026/1/7 以降）---
    if db_latest_str is not None:
        db_latest  = date.fromisoformat(db_latest_str)
        future_start = db_latest + timedelta(days=1)

        if future_start <= target_end:
            print(f"\n➡️  未来方向差分補完: {future_start} 〜 {target_end}")
            _run_bulk_fetch(db, future_start, target_end, label="差分")
        else:
            print(f"\n✅ 未来方向: 最終日 {db_latest} はすでに最新です。")

    # 完了確認
    new_oldest = db.get_oldest_saved_date()
    new_latest = db.get_latest_saved_date()
    print(f"\n{'='*40}")
    print(f"🎉 バックフィル完了")
    print(f"   DB最古日: {new_oldest}")
    print(f"   DB最終日: {new_latest}")
    if new_oldest and date.fromisoformat(new_oldest) <= target_start:
        print(f"   ✅ {BACKFILL_YEARS}年分のデータが揃いました！")
    else:
        print(f"   ⏳ まだ途中です。再度 Initial Backfill を実行してください。")
    print(f"{'='*40}")


def _run_bulk_fetch(
    db: database_manager.DBManager,
    start: date,
    end: date,
    label: str = "",
):
    """指定期間の全銘柄データを Yahoo Finance で一括取得して DB に保存。"""
    if start > end:
        print(f"  ⚠️ 取得範囲が逆転しています（{start} > {end}）。スキップ。")
        return

    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    start_str = start.strftime("%Y-%m-%d")
    end_str   = (end + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"  対象銘柄: {len(all_yf_tickers)} 銘柄")
    print(f"  取得期間: {start_str} 〜 {end_str}\n")

    df = _yf_fetch_bulk(all_yf_tickers, start_str, end_str, label=label)

    if df.empty:
        print(f"  ⚠️ [{label}] データが取得できませんでした。")
        return

    # 大量データを分割して保存（Supabase タイムアウト対策）
    batch_size = 50000  # 5万行ずつ保存
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i : i + batch_size]
        db.save_prices(chunk)
        print(f"  💾 保存: {i + len(chunk):,} / {len(df):,} 件")

    print(f"  ✅ [{label}] 合計 {len(df):,} 件保存完了")


# -----------------------------------------------------------------------
# エントリーポイント
# -----------------------------------------------------------------------
if __name__ == "__main__":
    backfill_data()
