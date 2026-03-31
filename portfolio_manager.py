"""
portfolio_manager.py
====================
株価データの取得・DB 同期を担当するモジュール。

【設計方針】
  - J-Quants Free プラン（5 req/min）を厳守しつつ最速で取得
  - DB の最終保存日から「続き」を取得するチェックポイント方式
  - J-Quants で取れない日・J-Quants より新しい日は Yahoo Finance で補完
  - DB 保存はバッチ処理でオーバーヘッドを削減

【差分取得の優先順位】
  1. 取得すべき日付範囲を「今日（JST）」を基準に決定
  2. J-Quants が持っている範囲 → J-Quants で取得
  3. J-Quants の最新日より新しい日 → Yahoo Finance で取得
  4. J-Quants が空（祝日等）→ Yahoo Finance で補完
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

# J-Quants の最新日確認に使うサンプル銘柄（複数用意して順番に試す）
_SAMPLE_CODES = ["72030", "86580", "90840", "30480"]

# J-Quants Free: 5 req/min → 最短 12.0 秒。0.5 秒の余裕をもって 12.5 秒
_JQUANTS_INTERVAL = float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "12.5"))

# DB バッチ保存のまとめ単位（日数）
_BATCH_DAYS = int(os.getenv("BATCH_DAYS", "5"))

# Supabase の Statement Timeout 対策: 1000 行ずつ分割 INSERT
_DB_CHUNK_SIZE = int(os.getenv("DB_CHUNK_SIZE", "1000"))

# Yahoo Finance の 1 リクエストあたり最大銘柄数
_YF_CHUNK_SIZE = int(os.getenv("YF_CHUNK_SIZE", "100"))

# Yahoo Finance チャンク間の待機秒数
_YF_SLEEP = float(os.getenv("YF_SLEEP_SEC", "2.0"))


# -----------------------------------------------------------------------
# 今日の日付（JST 基準）
# -----------------------------------------------------------------------
def _today_jst() -> date:
    """GitHub Actions は UTC なので JST（+9）に変換して今日の日付を返す。"""
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


# -----------------------------------------------------------------------
# J-Quants レートリミッター
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
# 銘柄コードのユーティリティ
# -----------------------------------------------------------------------
def _to_yf_ticker(code: str) -> str:
    """
    任意の形式のコードを Yahoo Finance 形式（4桁.T）に変換する。
    例: "30480.T" → "3048.T" / "3048" → "3048.T"
    """
    code_only = code.replace(".T", "").strip()
    if len(code_only) > 4:
        code_only = code_only[:4]
    return f"{code_only}.T"


def _to_db_ticker(yf_ticker: str) -> str:
    """Yahoo Finance 形式（4桁.T）を DB 保存形式に変換（そのまま 4桁.T）。"""
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
    """
    複数のサンプル銘柄を順番に試して「J-Quants で取得可能な最新日」を返す。
    すべて失敗した場合は None を返す。

    ※ Free プランでは銘柄によって取得できる範囲が異なる場合があるため
      複数銘柄で試して最も新しい日付を採用する。
    """
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
            continue

    return best_date


def _jq_fetch_day(api_key: str, date_str: str, limiter: RateLimiter) -> list[dict]:
    """
    date 指定で 1 日分の全銘柄データを取得（pagination 対応）。
    400 → [] / 200 で空 → [] / 429 → 自動リトライ
    """
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
    """J-Quants レスポンスを DB 保存用 DataFrame に変換。ticker は 4桁.T 形式。"""
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
# Yahoo Finance フォールバック（一括高速取得）
# -----------------------------------------------------------------------
def _yf_fetch_range(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    Yahoo Finance から start〜end の株価を一括取得（4桁.T 形式）。
    _YF_CHUNK_SIZE 件ずつ分割してタイムアウトを防ぐ。
    """
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
    """start〜end の土日を除いた日付リストを返す（昇順）。"""
    days = []
    d    = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


# -----------------------------------------------------------------------
# メイン同期関数
# -----------------------------------------------------------------------
def sync_data():
    """
    DB の最終保存日を確認し、不足分だけ取得して保存する。

    【取得戦略】
      取得すべき最終日 = 今日（JST）の前営業日
      ├── J-Quants の最新日 以前  → J-Quants で取得（空なら Yahoo Finance 補完）
      └── J-Quants の最新日 より後 → Yahoo Finance で取得

    この方式により、J-Quants の最新日が DB より古くても
    Yahoo Finance で最新データを取得できる。
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db      = database_manager.DBManager()
    limiter = RateLimiter()

    # ---- 取得すべき最終日 = 前営業日（JST 基準）----
    today      = _today_jst()
    target_end = today - timedelta(days=1)
    while target_end.weekday() >= 5:   # 土日なら金曜日まで戻す
        target_end -= timedelta(days=1)

    # ---- J-Quants の取得可能最新日を確認 ----
    print("🔍 J-Quants 取得可能最新日を確認中...")
    jq_latest = _jq_latest_date(api_key, limiter)
    if jq_latest:
        print(f"✅ J-Quants 最新日: {jq_latest}")
    else:
        print("⚠️ J-Quants 最新日を取得できませんでした。Yahoo Finance のみで取得します。")
        jq_latest = date(2000, 1, 1)   # 実質的に全日程を Yahoo Finance で取得

    # ---- DB の最終保存日を確認 ----
    db_latest_str = db.get_latest_saved_date()

    if db_latest_str is None:
        start_date = target_end - timedelta(days=BACKFILL_YEARS * 365)
        total_est  = BACKFILL_YEARS * 250
        print(f"\n🆕 初回バックフィル開始")
        print(f"   取得範囲: {start_date} 〜 {target_end}（{BACKFILL_YEARS}年分）")
        print(f"   ⏱ 所要時間目安: {total_est*25//3600}〜{total_est*40//3600}時間")
        print(f"   ✅ 途中停止しても次回実行時に自動で続きから再開します")
    else:
        db_latest  = date.fromisoformat(db_latest_str)
        start_date = db_latest + timedelta(days=1)
        if start_date > target_end:
            print(f"✅ DB はすでに最新です（最終日: {db_latest}、取得対象最終日: {target_end}）。スキップします。")
            return
        print(f"\n🔄 差分取得: {start_date} 〜 {target_end}（DB最終日: {db_latest}）")

    # ---- 取得対象の日付リスト（土日除く）----
    target_days = _business_days(start_date, target_end)
    total_days  = len(target_days)
    if total_days == 0:
        print("✅ 取得対象の営業日がありません。スキップします。")
        return
    print(f"📅 取得対象: {total_days} 営業日")
    print(f"   J-Quants 担当: {start_date} 〜 {min(jq_latest, target_end)}")
    print(f"   Yahoo Finance 担当: {max(start_date, jq_latest + timedelta(days=1))} 〜 {target_end}\n")

    # ---- Yahoo Finance フォールバック用銘柄リスト ----
    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    saved_total  = 0
    batch_buffer = []
    skipped_days = []

    for idx, target_date in enumerate(target_days, 1):
        d_str    = target_date.strftime("%Y-%m-%d")
        progress = f"[{idx}/{total_days}]"

        print(f"{progress} 📥 {d_str}", end="  ", flush=True)

        if target_date <= jq_latest:
            # ---- J-Quants で取得（担当範囲）----
            rows = _jq_fetch_day(api_key, d_str, limiter)

            if rows:
                df = _jq_rows_to_df(rows)
                batch_buffer.append(df)
                print(f"✅ J-Quants {len(df):,}件")
            else:
                # J-Quants が空（祝日 or 200で空）→ Yahoo Finance 補完
                print(f"⚠️ J-Quants空 → Yahoo Finance補完中...")
                yf_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
                df_yf  = _yf_fetch_range(all_yf_tickers, d_str, yf_end)
                if not df_yf.empty:
                    batch_buffer.append(df_yf)
                    print(f"  ✅ Yahoo Finance {len(df_yf):,}件")
                else:
                    skipped_days.append(d_str)
                    print(f"  ℹ️ データなし（祝日の可能性）→ スキップ")
        else:
            # ---- Yahoo Finance で取得（J-Quants より新しい範囲）----
            print(f"📡 Yahoo Finance（J-Quants範囲外）", end="  ", flush=True)
            yf_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
            df_yf  = _yf_fetch_range(all_yf_tickers, d_str, yf_end)
            if not df_yf.empty:
                batch_buffer.append(df_yf)
                print(f"✅ {len(df_yf):,}件")
            else:
                skipped_days.append(d_str)
                print(f"ℹ️ データなし → スキップ")

        # ---- バッチ保存 ----
        if len(batch_buffer) >= _BATCH_DAYS:
            combined     = pd.concat(batch_buffer, ignore_index=True)
            db.save_prices(combined)
            saved_total  += len(combined)
            batch_buffer  = []
            print(f"  💾 バッチ保存: 累計 {saved_total:,} 件")

    # ---- 残りのバッファを保存 ----
    if batch_buffer:
        combined     = pd.concat(batch_buffer, ignore_index=True)
        db.save_prices(combined)
        saved_total += len(combined)
        print(f"\n💾 最終バッチ保存: {len(combined):,} 件")

    # ---- 完了サマリー ----
    print(f"\n{'='*40}")
    print(f"🎉 同期完了")
    print(f"   保存件数  : {saved_total:,} 件")
    print(f"   取得日数  : {total_days} 営業日")
    if skipped_days:
        print(f"   スキップ  : {len(skipped_days)} 日（祝日等）")
    print(f"{'='*40}")


if __name__ == "__main__":
    sync_data()
