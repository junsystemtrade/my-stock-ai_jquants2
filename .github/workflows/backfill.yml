"""
portfolio_manager.py
====================
株価データの取得・DB 同期を担当するモジュール。

【設計方針】
  - J-Quants Free プラン（5 req/min）を厳守しつつ最速で取得
  - DB の最終保存日から「続き」を取得するチェックポイント方式
    → GitHub Actions 6 時間制限内に終わらなくても翌回に続きから再開できる
  - J-Quants で取れない日（祝日/非取引日）は Yahoo Finance で一括補完
  - DB 保存はバッチ処理（複数日まとめて INSERT）でオーバーヘッドを削減

【初回バックフィルの所要時間見積もり（Free プラン）】
  3 年 ≒ 750 営業日
  1 日あたり約 2〜3 ページ × 12.5 秒 = 約 25〜38 秒
  合計: 約 5〜8 時間 → 複数回実行で完了（チェックポイントで続きから再開）

【差分更新（2 回目以降）】
  1〜数日分のみ取得 → 数十秒〜数分で完了
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
BACKFILL_YEARS    = 3           # 初回バックフィルの年数（3年）
SAMPLE_CODE       = "30480"     # 取得可能最新日の確認用サンプル銘柄（ビックカメラ）

# J-Quants Free: 5 req/min → 最短 12.0 秒。余裕を 0.5 秒だけ取って 12.5 秒
_JQUANTS_INTERVAL = float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "12.5"))

# DB バッチ保存のまとめ単位（日数）
_BATCH_DAYS = int(os.getenv("BATCH_DAYS", "5"))

# Yahoo Finance の 1 リクエストあたり最大銘柄数（多すぎるとタイムアウト）
_YF_CHUNK_SIZE = int(os.getenv("YF_CHUNK_SIZE", "100"))

# Yahoo Finance リクエスト間の待機秒数（無料・非公式 API への負荷対策）
_YF_SLEEP = float(os.getenv("YF_SLEEP_SEC", "2.0"))


# -----------------------------------------------------------------------
# J-Quants レートリミッター
# -----------------------------------------------------------------------
class RateLimiter:
    """
    Free プランの 5 req/min を確実に守る。
    monotonic clock で計測して sleep をギリギリまで詰める。
    """
    def __init__(self, min_interval_sec: float = _JQUANTS_INTERVAL):
        self.min_interval = min_interval_sec
        self._last        = 0.0

    def wait(self):
        elapsed = time.monotonic() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


# -----------------------------------------------------------------------
# JPX 上場銘柄マスタ取得
# -----------------------------------------------------------------------
def get_target_tickers() -> dict:
    """
    JPX の上場銘柄一覧を取得し {ticker: {"name": str}} の辞書を返す。
    例) {"30480.T": {"name": "ビックカメラ"}, ...}
    jpx_master モジュールがあればそちらを優先する。
    """
    try:
        import jpx_master
        return jpx_master.get_target_tickers()
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
            if len(code) == 4 and code.isdigit():
                stock_map[f"{code}.T"] = {"name": name}

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


def _jq_latest_date(api_key: str, limiter: RateLimiter) -> date:
    """
    サンプル銘柄（code 指定）の末尾 Date を「Free で取得可能な最新日」として返す。
    1 リクエストだけで済む最速の方法。
    """
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    limiter.wait()
    r = requests.get(
        url,
        headers=_jq_headers(api_key),
        params={"code": SAMPLE_CODE},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        raise RuntimeError(
            "J-Quants: サンプル銘柄データが空です。APIキーまたはプランを確認してください。"
        )
    return pd.to_datetime(data[-1]["Date"]).date()


def _jq_fetch_day(api_key: str, date_str: str, limiter: RateLimiter) -> list[dict]:
    """
    date 指定で 1 日分の全銘柄データを取得（pagination 対応）。

    戻り値:
      - データあり  → list[dict]
      - 非取引日/範囲外（400） → []
      - 429 → Retry-After 秒待って自動リトライ
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
            print(f"  ⚠️ 429 Too Many Requests → {retry_after}秒待機")
            time.sleep(retry_after)
            continue

        if r.status_code == 400:
            return []   # 非取引日 or プラン範囲外

        r.raise_for_status()

        payload        = r.json()
        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            break

    return rows


def _jq_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    """J-Quants レスポンスを DB 保存用 DataFrame に変換。"""
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
# Yahoo Finance フォールバック（一括高速取得）
# -----------------------------------------------------------------------
def _yf_fetch_range(
    tickers: list[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Yahoo Finance から start〜end の株価を一括取得。
    銘柄数が多い場合は _YF_CHUNK_SIZE 件ずつ分割してタイムアウトを防ぐ。
    1 リクエストで複数銘柄・複数日を取得できるため J-Quants より大幅に高速。

    tickers : ["1234.T", "5678.T", ...] 形式
    start   : "YYYY-MM-DD"
    end     : "YYYY-MM-DD"（取得したい最終日の翌日）
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
                threads=True,   # yfinance 組み込みの並列取得
                timeout=30,
            )
        except Exception as e:
            print(f"  ❌ Yahoo Finance チャンク取得失敗: {e}")
            time.sleep(_YF_SLEEP)
            continue

        if raw is None or raw.empty:
            time.sleep(_YF_SLEEP)
            continue

        # 単一銘柄の場合は MultiIndex にならない
        if len(chunk) == 1:
            ticker = chunk[0]
            df     = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            for idx, row in df.iterrows():
                if pd.isna(row.get("Close")):
                    continue
                all_records.append({
                    "ticker": ticker,
                    "date":   idx.date(),
                    "open":   row.get("Open"),
                    "high":   row.get("High"),
                    "low":    row.get("Low"),
                    "price":  row.get("Close"),
                    "volume": row.get("Volume"),
                })
        else:
            for ticker in chunk:
                try:
                    df = raw[ticker].dropna(how="all")
                except KeyError:
                    continue
                for idx, row in df.iterrows():
                    if pd.isna(row.get("Close")):
                        continue
                    all_records.append({
                        "ticker": ticker,
                        "date":   idx.date(),
                        "open":   row.get("Open"),
                        "high":   row.get("High"),
                        "low":    row.get("Low"),
                        "price":  row.get("Close"),
                        "volume": row.get("Volume"),
                    })

        time.sleep(_YF_SLEEP)   # Yahoo Finance への過負荷防止

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

    【初回（DB が空）】
      過去 BACKFILL_YEARS 年分を J-Quants から日単位で取得。
      J-Quants が空の日は Yahoo Finance で補完。
      GitHub Actions 6 時間制限を超えても次回実行時に続きから再開できる。

    【2 回目以降（差分）】
      DB 最終日の翌日〜今日分だけ取得（通常数十秒〜数分で完了）。

    【高速化ポイント】
      1. レート間隔を 12.5 秒に詰める（Free 上限 12.0 秒ギリギリ）
      2. DB 保存を _BATCH_DAYS 日ごとのバッチ処理でオーバーヘッド削減
      3. Yahoo Finance は threads=True + チャンク分割で並列一括取得
      4. チェックポイント方式で途中再開可能
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db      = database_manager.DBManager()
    limiter = RateLimiter()

    # ---- J-Quants の取得可能最新日を確認（1 リクエスト消費）----
    print("🔍 J-Quants 取得可能最新日を確認中...")
    jq_latest = _jq_latest_date(api_key, limiter)
    print(f"✅ J-Quants 最新日: {jq_latest}")

    # ---- DB の最終保存日を確認 ----
    db_latest_str = db.get_latest_saved_date()

    if db_latest_str is None:
        # 初回: 3 年前から取得
        start_date = jq_latest - timedelta(days=BACKFILL_YEARS * 365)
        total_est  = BACKFILL_YEARS * 250  # 営業日概算
        hours_low  = total_est * 25 // 3600
        hours_high = total_est * 40 // 3600
        print(f"🆕 初回バックフィル開始")
        print(f"   取得範囲: {start_date} 〜 {jq_latest}（{BACKFILL_YEARS}年分）")
        print(f"   ⏱ 所要時間目安: {hours_low}〜{hours_high}時間")
        print(f"   ✅ 途中停止しても次回実行時に続きから自動再開します")
    else:
        db_latest  = date.fromisoformat(db_latest_str)
        start_date = db_latest + timedelta(days=1)
        if start_date > jq_latest:
            print(f"✅ DB はすでに最新です（最終日: {db_latest}）。スキップします。")
            return
        diff_days = (jq_latest - db_latest).days
        print(f"🔄 差分取得: {start_date} 〜 {jq_latest}（{diff_days}日分）")

    # ---- 取得対象の日付リスト（土日除く）----
    target_days = _business_days(start_date, jq_latest)
    total_days  = len(target_days)
    print(f"📅 取得対象: {total_days} 営業日\n")

    # ---- Yahoo Finance フォールバック用銘柄リスト ----
    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())

    saved_total  = 0
    batch_buffer = []   # バッチ保存バッファ（_BATCH_DAYS 日分溜めてから一括 INSERT）
    skipped_days = []   # 祝日など取得できなかった日（ログ用）

    for idx, target_date in enumerate(target_days, 1):
        d_str    = target_date.strftime("%Y-%m-%d")
        progress = f"[{idx}/{total_days}]"

        print(f"{progress} 📥 {d_str}", end="  ", flush=True)

        # -- J-Quants で取得 --
        rows = _jq_fetch_day(api_key, d_str, limiter)

        if rows:
            df = _jq_rows_to_df(rows)
            batch_buffer.append(df)
            print(f"✅ J-Quants {len(df):,}件")

        else:
            # J-Quants が空 → Yahoo Finance で補完
            print(f"⚠️ J-Quants空 → Yahoo Finance補完中...")
            yf_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
            df_yf  = _yf_fetch_range(all_yf_tickers, d_str, yf_end)

            if not df_yf.empty:
                batch_buffer.append(df_yf)
                print(f"  ✅ Yahoo Finance {len(df_yf):,}件")
            else:
                skipped_days.append(d_str)
                print(f"  ℹ️ データなし（祝日の可能性）→ スキップ")

        # ---- バッチ保存（_BATCH_DAYS 日分たまったら保存）----
        if len(batch_buffer) >= _BATCH_DAYS:
            combined    = pd.concat(batch_buffer, ignore_index=True)
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
