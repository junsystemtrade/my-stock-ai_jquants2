import os
import io
import time
import gzip
import requests
import pandas as pd
from datetime import date, timedelta
import database_manager

BASE_API = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"  # Bulkで指定する endpoint 名 [1](https://jpx-jquants.com/en/spec/bulk-list)


def _headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Accept": "application/json"}


# ---------------------------
# Bulk API ルート（最優先）
# ---------------------------
def _bulk_list_files(api_key: str) -> list[dict]:
    """
    /v2/bulk/list に endpoint を渡すと、プランで許される全期間ぶんのファイル一覧が返る [1](https://jpx-jquants.com/en/spec/bulk-list)
    """
    url = f"{BASE_API}/bulk/list"
    r = requests.get(url, headers=_headers(api_key), params={"endpoint": EQ_DAILY_ENDPOINT}, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


def _bulk_get_download_url(api_key: str, key: str) -> str:
    """
    /v2/bulk/get に key を渡すと、署名付きURLが返る（有効期限5分）[3](https://jpx-jquants.com/en/spec/bulk-get)[4](https://jpx-jquants.com/en/spec/bulk-get.md)
    """
    url = f"{BASE_API}/bulk/get"
    r = requests.get(url, headers=_headers(api_key), params={"key": key}, timeout=30)
    r.raise_for_status()
    return r.json()["url"]


def _read_csv_gz_from_url(download_url: str, chunksize: int = 200_000):
    """
    署名付きURLから .csv.gz をストリームで読み、pandas の chunksize で分割処理する。
    （URLは短命なので、取得したらすぐ読みに行く）[3](https://jpx-jquants.com/en/spec/bulk-get)[4](https://jpx-jquants.com/en/spec/bulk-get.md)
    """
    # ダウンロードをメモリに溜めずにストリームでgzip解凍
    with requests.get(download_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        gz = gzip.GzipFile(fileobj=r.raw)
        # pandasはfile-like objectを読める
        for chunk in pd.read_csv(gz, chunksize=chunksize):
            yield chunk


def _normalize_equities_bars_daily_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bulk CSVの列名は環境/仕様で変わる可能性があるため、両対応する
    - APIレスポンス: Date, Code, O,H,L,C,Vo ... [6](https://jpx-jquants.com/ja/spec)
    - CSVも同等の列が基本
    """
    # よくある列名候補を吸収
    # Date/Code が無い場合は例外にする（取り込みを止めた方が安全）
    if "Date" not in df.columns or "Code" not in df.columns:
        raise ValueError(f"CSVの列が想定外です。columns={list(df.columns)}")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["Date"]).dt.date
    out["ticker"] = df["Code"].astype(str)

    # 価格列（存在するものだけ）
    col_map = {
        "O": "open",
        "H": "high",
        "L": "low",
        "C": "price",
        "Vo": "volume",
    }
    for src, dst in col_map.items():
        if src in df.columns:
            out[dst] = df[src]
        else:
            # 欠けている場合は NaN を入れる（API仕様上 Nullあり得る）[6](https://jpx-jquants.com/ja/spec)
            out[dst] = pd.NA

    # 必須カラムに整形
    out = out[["ticker", "date", "open", "high", "low", "price", "volume"]]
    out = out.drop_duplicates(subset=["ticker", "date"])
    return out


def _sync_via_bulk(api_key: str, db: database_manager.DBManager):
    """
    Bulk APIで取れるだけ全部取り込む
    """
    files = _bulk_list_files(api_key)  # プランで許可された全期間ファイル [1](https://jpx-jquants.com/en/spec/bulk-list)
    if not files:
        print("⚠️ Bulk: ダウンロード可能ファイルがありません（プラン/期間の可能性）")
        return

    # 古い順に処理したい場合は Key の年月でソート（Keyに年/月が含まれる）[1](https://jpx-jquants.com/en/spec/bulk-list)
    # ここでは LastModified ではなく Key 文字列でざっくり昇順
    files_sorted = sorted(files, key=lambda x: x.get("Key", ""))

    print(f"📦 Bulk: 取り込み対象ファイル数 = {len(files_sorted)}")

    for i, f in enumerate(files_sorted, 1):
        key = f["Key"]
        print(f"📥 [{i}/{len(files_sorted)}] Bulkファイル取得: {key}")

        # 署名付きURL取得（5分で失効）[3](https://jpx-jquants.com/en/spec/bulk-get)[4](https://jpx-jquants.com/en/spec/bulk-get.md)
        download_url = _bulk_get_download_url(api_key, key)

        total_rows = 0
        for chunk in _read_csv_gz_from_url(download_url, chunksize=200_000):
            norm = _normalize_equities_bars_daily_df(chunk)
            db.save_prices(norm)  # DB側で chunksize 設定しているとさらに安定
            total_rows += len(norm)

        print(f"✅ Bulk取り込み完了: {key} / rows={total_rows}")

        # 連続アクセス抑制（プランに応じて調整）
        time.sleep(0.5)


# ---------------------------
# フォールバック（日次APIで遡る）
# ---------------------------
def _fetch_all_issues_for_date(api_key: str, date_str: str) -> list[dict]:
    """
    date指定で全銘柄（その日）を取得。pagination_keyで完走 [6](https://jpx-jquants.com/ja/spec)
    """
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    rows = []
    pagination_key = None

    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        r = requests.get(url, headers=_headers(api_key), params=params, timeout=30)

        if r.status_code == 429:
            time.sleep(30)
            continue

        r.raise_for_status()
        payload = r.json()

        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            break

        time.sleep(0.2)

    return rows


def _find_latest_trading_date_str(api_key: str, lookback_days: int = 20) -> str:
    """
    直近で200が返る日付を探す（YYYYMMDD or YYYY-MM-DD はどちらも仕様上OK）[6](https://jpx-jquants.com/ja/spec)
    """
    d = date.today() - timedelta(days=1)
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"

    for _ in range(lookback_days):
        while d.weekday() >= 5:
            d -= timedelta(days=1)

        date_str = d.strftime("%Y%m%d")
        r = requests.get(url, headers=_headers(api_key), params={"date": date_str}, timeout=20)

        if r.status_code == 200:
            return date_str

        if r.status_code in (400, 429):
            d -= timedelta(days=1)
            continue

        r.raise_for_status()

    raise RuntimeError("最新営業日を判定できませんでした")


def _sync_via_daily_api_backfill(api_key: str, db: database_manager.DBManager):
    """
    Bulkが使えない時のフォールバック：
    date指定で全銘柄を取りつつ、日付を遡って「取れるだけ」続ける
    """
    d_str = _find_latest_trading_date_str(api_key)
    d = pd.to_datetime(d_str).date()
    print(f"📅 フォールバック開始: latest={d_str}")

    while True:
        date_str = d.strftime("%Y%m%d")
        print(f"📅 取得中: {date_str}")

        try:
            rows = _fetch_all_issues_for_date(api_key, date_str)
        except requests.HTTPError as e:
            # 400になったら「これ以上古いデータが取れない」可能性があるので終了
            if "400" in str(e):
                print("⛔ フォールバック: これ以上過去は取得できない可能性があるため終了")
                break
            raise

        if not rows:
            print("⚠️ データなし → 終了")
            break

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df["ticker"] = df["Code"].astype(str)

        df = df.rename(columns={"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"})
        df = df[["ticker", "date", "open", "high", "low", "price", "volume"]].drop_duplicates(subset=["ticker", "date"])

        db.save_prices(df)
        print(f"✅ 保存完了: {len(df)} 行")

        d -= timedelta(days=1)
        time.sleep(1.0)


def sync_data():
    """
    エントリポイント（GitHub Actions から呼ばれる）
    1) Bulk API が使えるなら、取れるだけの過去を一気に全部投入
    2) 使えないなら、日次API(date指定)で遡って取れるだけ取る
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db = database_manager.DBManager()

    print("🚀 過去データ全力取得モード開始（可能ならBulk優先）")

    # Bulkは Light以上で利用可能 [5](https://jpx-jquants.com/en/spec/bulk.md)
    try:
        _sync_via_bulk(api_key, db)
        print("🎉 Bulkルート完了")
        return
    except requests.HTTPError as e:
        # Bulkが権限的に使えない/契約外ならフォールバック
        print(f"⚠️ Bulk利用不可または失敗: {e} → フォールバックへ切替")

    _sync_via_daily_api_backfill(api_key, db)
    print("🎉 フォールバック完了")


if __name__ == "__main__":
    sync_data()
