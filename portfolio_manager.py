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

【Gemini レビュー反映点】
  1. Yahoo Finance 銘柄コードを 4桁.T 形式に統一（30480.T → 3048.T）
  2. Yahoo Finance MultiIndex 処理を単一/複数銘柄で統一した汎用処理に変更
  3. DB バッチ保存に chunksize=1000 の分割 INSERT を追加（Supabase タイムアウト対策）

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

# DB バッチ保存のまとめ単位（日数）。大きすぎると Supabase がタイムアウトするので注意
_BATCH_DAYS = int(os.getenv("BATCH_DAYS", "5"))

# DB への INSERT を分割する単位（行数）。Supabase の Statement Timeout 対策
_DB_CHUNK_SIZE = int(os.getenv("DB_CHUNK_SIZE", "1000"))

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
# 銘柄コードのユーティリティ
# -----------------------------------------------------------------------
def _to_yf_ticker(code: str) -> str:
    """
    任意の形式の銘柄コードを Yahoo Finance 形式（4桁.T）に変換する。

    例:
      "30480.T" → "3048.T"  （5桁.T → 4桁.T）
      "3048.T"  → "3048.T"  （すでに正しい形式）
      "3048"    → "3048.T"  （サフィックスなし → .T 付与）
      "30480"   → "3048.T"  （5桁 → 4桁.T）
    """
    # .T を外してコード部分だけ取り出す
    code_only = code.replace(".T", "").strip()
    # 5桁以上なら先頭4桁に切り詰める（J-Quants は5桁コードを使うことがある）
    if len(code_only) > 4:
        code_only = code_only[:4]
    return f"{code_only}.T"


def _to_db_ticker(yf_ticker: str) -> str:
    """
    Yahoo Finance 形式（4桁.T）を DB 保存形式（4桁.T）のまま返す。
    J-Quants と統一するため、コード部分は4桁に揃える。
    """
    return _to_yf_ticker(yf_ticker)


# -----------------------------------------------------------------------
# JPX 上場銘柄マスタ取得
# -----------------------------------------------------------------------
def get_target_tickers() -> dict:
    """
    JPX の上場銘柄一覧を取得し {yf_ticker: {"name": str}} の辞書を返す。
    ticker は Yahoo Finance 形式（4桁.T）に統一する。
    例) {"3048.T": {"name": "ビックカメラ"}, ...}
    """
    try:
        import jpx_master
        raw = jpx_master.get_target_tickers()
        # jpx_master が返す ticker を Yahoo Finance 形式に変換
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
                # 4桁.T 形式に統一
                yf_ticker = f"{code[:4]}.T"
                stock_map[yf_ticker] = {"name": name}

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
      - データあり          → list[dict]（空でない）
      - 非取引日（400）     → []
      - 祝日（200 で空）    → []   ← Gemini 指摘: 200 で空が返るケースを考慮
      - 429                → Retry-After 秒待って自動リトライ
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
            # 土日など明確な非取引日
            return []

        if r.status_code in (403, 500, 503):
            # メンテナンス等 → 空で返してスキップ（深追いしない）
            print(f"  ⚠️ J-Quants {r.status_code} → スキップ")
            return []

        r.raise_for_status()

        payload        = r.json()
        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            break

    # 200 で返ってきても data が空 = 平日祝日の可能性
    # if rows: で判定しているので Yahoo Finance 補完に自動切り替えされる
    return rows


def _jq_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    """
    J-Quants レスポンスを DB 保存用 DataFrame に変換。
    ticker は 4桁.T 形式（Yahoo Finance と統一）。
    """
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["Date"]).dt.date

    # J-Quants の Code（例: "30480"）を 4桁.T 形式に変換
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
def _yf_fetch_range(
    tickers: list[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Yahoo Finance から start〜end の株価を一括取得。
    銘柄数が多い場合は _YF_CHUNK_SIZE 件ずつ分割してタイムアウトを防ぐ。

    【Gemini 指摘反映】
      - MultiIndex / 単一銘柄の両ケースを統一した汎用処理で吸収
      - ticker は 4桁.T 形式に統一（DB と J-Quants に合わせる）

    tickers : ["3048.T", "7203.T", ...] 形式（4桁.T）
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

        # ------------------------------------------------------------------
        # 【Gemini 指摘反映】MultiIndex / 単一銘柄を統一した汎用処理
        #
        # yf.download の返却形式:
        #   複数銘柄 → columns が MultiIndex (ticker, OHLCV)
        #   単一銘柄 → columns がフラット (Open, High, Low, Close, Volume)
        #
        # isinstance(raw.columns, pd.MultiIndex) で判定し、どちらも
        # 同じループで処理できるよう正規化する。
        # ------------------------------------------------------------------
        for ticker in chunk:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    # 複数銘柄: raw[ticker] でその銘柄のスライスを取得
                    df = raw[ticker].dropna(how="all")
                else:
                    # 単一銘柄: raw 自体がその銘柄のデータ
                    df = raw.dropna(how="all")

                for idx, row in df.iterrows():
                    close = row.get("Close")
                    if pd.isna(close):
                        continue
                    all_records.append({
                        # DB・J-Quants と揃えて 4桁.T 形式で保存
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
      J-Quants が空（400 / 200空）の日は Yahoo Finance で補完。
      GitHub Actions 6 時間制限を超えても次回実行時に続きから再開できる。

    【2 回目以降（差分）】
      DB 最終日の翌日〜今日分だけ取得（通常数十秒〜数分で完了）。

    【高速化ポイント】
      1. レート間隔を 12.5 秒に詰める（Free 上限 12.0 秒ギリギリ）
      2. DB 保存を _BATCH_DAYS 日ごとのバッチ処理でオーバーヘッド削減
      3. Supabase タイムアウト対策で _DB_CHUNK_SIZE=1000 行ずつ分割 INSERT
      4. Yahoo Finance は threads=True + チャンク分割で並列一括取得
      5. チェックポイント方式で途中再開可能
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
        print(f"   ✅ 途中停止しても次回実行時に自動で続きから再開します")
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

    # ---- Yahoo Finance フォールバック用銘柄リスト（4桁.T 形式に統一）----
    ticker_map     = get_target_tickers()
    all_yf_tickers = list(ticker_map.keys())   # すでに 4桁.T 形式

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
            # J-Quants が空（400 または 200空 = 平日祝日を含む）
            # → Yahoo Finance で補完
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
            combined = pd.concat(batch_buffer, ignore_index=True)
            db.save_prices(combined)   # 内部で chunksize=1000 分割 INSERT
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
