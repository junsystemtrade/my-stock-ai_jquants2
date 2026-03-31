"""
portfolio_manager.py
====================
株価データの取得・DB同期を担当するモジュール。

動作フロー:
  1. DBの最終保存日を確認（get_latest_saved_date）
  2. 初回（DBが空）→ 過去5年分を J-Quants でバックフィル
     2回目以降     → 前回保存日の翌日〜今日を差分取得
  3. J-Quants で取得できない日・銘柄は Yahoo Finance でフォールバック
  4. 取得データを DBManager.save_prices() で保存（重複はスキップ）

外部依存:
  - J-Quants API  (JQUANTS_API_KEY 環境変数)
  - Yahoo Finance (yfinance)
  - JPX 上場銘柄マスタ (jpx_master モジュール or 内蔵のスクレイピング)
  - database_manager.DBManager
"""

import io
import os
import time
import requests
import pandas as pd
import yfinance as yf

from datetime import date, timedelta
from typing import Optional

import database_manager

# -----------------------------------------------------------------------
# 定数
# -----------------------------------------------------------------------
BASE_API          = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"
BACKFILL_YEARS    = 5          # 初回バックフィルの年数
SAMPLE_CODE       = "30480"    # J-Quants の取得可能範囲確認用サンプル銘柄


# -----------------------------------------------------------------------
# レートリミッター（Free プラン: 5 req/min → 13 秒間隔）
# -----------------------------------------------------------------------
class RateLimiter:
    def __init__(self, min_interval_sec: float = 13.0):
        self.min_interval = float(min_interval_sec)
        self._last = 0.0

    def wait(self):
        now     = time.monotonic()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


# -----------------------------------------------------------------------
# JPX 上場銘柄マスタ取得
#   → jpx_master モジュールがあればそちらを使い、なければ内蔵スクレイピング
# -----------------------------------------------------------------------
def get_target_tickers() -> dict:
    """
    JPX の上場銘柄一覧を取得し {ticker: {"name": str}} の辞書を返す。
    例) {"30480.T": {"name": "ビックカメラ"}, ...}
    """
    try:
        import jpx_master  # リポジトリに同梱されている場合はそちらを優先
        return jpx_master.get_target_tickers()
    except ImportError:
        pass

    # ---- 内蔵スクレイピング（jpx_master がない場合）----
    from bs4 import BeautifulSoup

    base_url  = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers   = {"User-Agent": "Mozilla/5.0"}

    try:
        res  = requests.get(list_page, headers=headers, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)
        if not link:
            return {}

        excel_url = base_url + link["href"]
        resp      = requests.get(excel_url, headers=headers, timeout=60)
        df        = pd.read_excel(io.BytesIO(resp.content))
        df.columns = [str(c).strip() for c in df.columns]

        stock_map = {}
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip()
            name = str(row.iloc[2]).strip()
            if len(code) == 4 and code.isdigit():
                stock_map[f"{code}.T"] = {"name": name}
        return stock_map

    except Exception as e:
        print(f"❌ JPX銘柄マスタ取得失敗: {e}")
        return {}


# -----------------------------------------------------------------------
# J-Quants ヘルパー
# -----------------------------------------------------------------------
def _jquants_headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Accept": "application/json"}


def _jquants_latest_accessible_date(api_key: str, limiter: RateLimiter) -> date:
    """
    サンプル銘柄 (code指定) で全期間データを取得し、
    末尾の Date を「Free プランで取れる最新日」として返す。
    """
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    limiter.wait()
    r = requests.get(
        url,
        headers=_jquants_headers(api_key),
        params={"code": SAMPLE_CODE},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        raise RuntimeError("J-Quants: サンプル銘柄のデータが空です（APIキーまたはプランを確認）")
    return pd.to_datetime(data[-1]["Date"]).date()


def _jquants_fetch_day(api_key: str, date_str: str, limiter: RateLimiter) -> list[dict]:
    """
    date 指定で1日の全銘柄データを取得（pagination 対応）。
    400（非取引日/プラン範囲外）→ 空リストを返す。
    429（レート超過）→ Retry-After 待ちで再試行。
    """
    url            = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    rows: list     = []
    pagination_key = None

    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        limiter.wait()
        r = requests.get(url, headers=_jquants_headers(api_key), params=params, timeout=30)

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "60"))
            print(f"⚠️  J-Quants 429 → {retry_after}秒待機")
            time.sleep(retry_after)
            continue

        if r.status_code == 400:
            return []  # 非取引日 or プラン範囲外

        r.raise_for_status()

        payload        = r.json()
        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            break

    return rows


def _jquants_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    """
    J-Quants レスポンスを DB 保存用 DataFrame に変換。
    カラム: ticker, date, open, high, low, price, volume
    """
    df = pd.DataFrame(rows)
    df["date"]   = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = df["Code"].astype(str)

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
def _yahoo_fetch_range(
    tickers: list[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Yahoo Finance から start〜end の株価を一括取得し、
    DB 保存用 DataFrame に変換して返す。
    tickers は "1234.T" 形式。
    """
    if not tickers:
        return pd.DataFrame()

    print(f"📡 Yahoo Finance フォールバック: {len(tickers)} 銘柄 / {start}〜{end}")

    try:
        raw = yf.download(
            tickers,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
        )
    except Exception as e:
        print(f"❌ Yahoo Finance 取得失敗: {e}")
        return pd.DataFrame()

    records = []

    # 単一銘柄の場合は MultiIndex にならないので個別処理
    if len(tickers) == 1:
        ticker = tickers[0]
        df     = raw.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        for idx, row in df.iterrows():
            records.append({
                "ticker": ticker,
                "date":   idx.date(),
                "open":   row.get("Open"),
                "high":   row.get("High"),
                "low":    row.get("Low"),
                "price":  row.get("Close"),
                "volume": row.get("Volume"),
            })
    else:
        for ticker in tickers:
            try:
                df = raw[ticker].dropna(how="all")
            except KeyError:
                continue
            for idx, row in df.iterrows():
                records.append({
                    "ticker": ticker,
                    "date":   idx.date(),
                    "open":   row.get("Open"),
                    "high":   row.get("High"),
                    "low":    row.get("Low"),
                    "price":  row.get("Close"),
                    "volume": row.get("Volume"),
                })

    if not records:
        return pd.DataFrame()

    df_out = pd.DataFrame(records).drop_duplicates(subset=["ticker", "date"])
    return df_out


# -----------------------------------------------------------------------
# 取得対象の日付リストを生成（土日除く）
# -----------------------------------------------------------------------
def _business_days(start: date, end: date) -> list[date]:
    """
    start〜end の範囲で土日を除いた日付リストを返す。
    ※ 日本の祝日は J-Quants の 400 レスポンスで自動スキップされる。
    """
    days = []
    d    = start
    while d <= end:
        if d.weekday() < 5:  # 0=月〜4=金
            days.append(d)
        d += timedelta(days=1)
    return days


# -----------------------------------------------------------------------
# メイン同期関数
# -----------------------------------------------------------------------
def sync_data():
    """
    DBの最終保存日を確認し、不足している分だけ J-Quants から取得・保存する。

    - 初回（DB が空）      : 過去 BACKFILL_YEARS 年分を全日程取得
    - 2回目以降（差分）   : 前回保存日の翌日〜 J-Quants 最新日まで取得
    - J-Quants で空の日   : Yahoo Finance でフォールバック取得
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db      = database_manager.DBManager()
    limiter = RateLimiter(
        min_interval_sec=float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "13"))
    )

    # ---- J-Quants で取得可能な最新日を確認 ----
    print("🔍 J-Quants から取得可能な最新日を確認中...")
    jquants_latest = _jquants_latest_accessible_date(api_key, limiter)
    print(f"✅ J-Quants 最新日: {jquants_latest}")

    # ---- DB の最終保存日を確認 ----
    db_latest_str = db.get_latest_saved_date()

    if db_latest_str is None:
        # 初回: 5年前から取得
        start_date = jquants_latest - timedelta(days=BACKFILL_YEARS * 365)
        print(f"🆕 初回バックフィル: {start_date} 〜 {jquants_latest}（約{BACKFILL_YEARS}年分）")
    else:
        db_latest  = date.fromisoformat(db_latest_str)
        start_date = db_latest + timedelta(days=1)
        print(f"🔄 差分取得: {start_date} 〜 {jquants_latest}（DB最終日: {db_latest}）")

    if start_date > jquants_latest:
        print("✅ DB はすでに最新です。取得スキップ。")
        return

    # ---- 取得対象の日付リスト（土日除く）----
    target_days = _business_days(start_date, jquants_latest)
    print(f"📅 取得対象日数: {len(target_days)} 日")

    # ---- Yahoo Finance フォールバック用に銘柄マスタを取得 ----
    ticker_map     = get_target_tickers()  # {"1234.T": {"name": "..."}, ...}
    all_yf_tickers = list(ticker_map.keys())

    saved_total = 0

    for target_date in target_days:
        d_str = target_date.strftime("%Y-%m-%d")
        print(f"\n📥 取得中: {d_str}")

        # -- J-Quants で取得 --
        rows = _jquants_fetch_day(api_key, d_str, limiter)

        if rows:
            df = _jquants_rows_to_df(rows)
            db.save_prices(df)
            saved_total += len(df)
            print(f"  ✅ J-Quants: {len(df)} 件保存")

        else:
            # J-Quants で取得できない場合（祝日 or プラン範囲外）
            # → Yahoo Finance でその1日分を取得
            print(f"  ⚠️  J-Quants 取得不可 → Yahoo Finance フォールバック")

            # Yahoo Finance は end が「その翌日」である必要がある
            yf_start = d_str
            yf_end   = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

            df_yf = _yahoo_fetch_range(all_yf_tickers, yf_start, yf_end)

            if not df_yf.empty:
                db.save_prices(df_yf)
                saved_total += len(df_yf)
                print(f"  ✅ Yahoo Finance: {len(df_yf)} 件保存")
            else:
                print(f"  ℹ️  {d_str} は取引なし（祝日）→ スキップ")

    print(f"\n🎉 同期完了: 合計 {saved_total} 件保存")


if __name__ == "__main__":
    sync_data()
