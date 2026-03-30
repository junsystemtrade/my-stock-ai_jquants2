import os
import io
import time
import gzip
import requests
import pandas as pd
from datetime import date, timedelta
import database_manager

BASE_API = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"

def _headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Accept": "application/json"}

# ---------------------------
# Bulk API (Lightプラン以上用)
# ---------------------------
def _bulk_list_files(api_key: str) -> list[dict]:
    url = f"{BASE_API}/bulk/list"
    r = requests.get(url, headers=_headers(api_key), params={"endpoint": EQ_DAILY_ENDPOINT}, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])

def _bulk_get_download_url(api_key: str, key: str) -> str:
    url = f"{BASE_API}/bulk/get"
    r = requests.get(url, headers=_headers(api_key), params={"key": key}, timeout=30)
    r.raise_for_status()
    return r.json()["url"]

def _read_csv_gz_from_url(download_url: str, chunksize: int = 100_000):
    with requests.get(download_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for chunk in pd.read_csv(gz, chunksize=chunksize):
                yield chunk

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if "Date" not in df.columns or "Code" not in df.columns:
        raise ValueError(f"想定外のカラム構成です: {df.columns}")
    
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["Date"]).dt.date
    out["ticker"] = df["Code"].astype(str)
    
    col_map = {"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"}
    for src, dst in col_map.items():
        out[dst] = df[src] if src in df.columns else pd.NA
        
    return out[["ticker", "date", "open", "high", "low", "price", "volume"]].drop_duplicates()

# ---------------------------
# Daily API (Free/Lightプラン用フォールバック)
# ---------------------------
def _find_latest_trading_date_str(api_key: str, lookback_days: int = 30) -> str:
    """
    ディレイを考慮し、実際にデータが取得できる最新の営業日を特定する
    """
    # 念のため昨日から探索開始
    d = date.today() - timedelta(days=1)
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"

    print(f"🔍 データが存在する最新営業日を探索中 (起点: {d})...")

    for _ in range(lookback_days):
        if d.weekday() >= 5: # 土日はスキップ
            d -= timedelta(days=1)
            continue
            
        # V2ではハイフン付き YYYY-MM-DD が確実
        date_str = d.strftime("%Y-%m-%d")
        try:
            r = requests.get(url, headers=_headers(api_key), params={"date": date_str}, timeout=20)
            if r.status_code == 200:
                print(f"✨ ヒットしました: {date_str}")
                return date_str
            print(f"  - {date_str}: {r.status_code}")
        except Exception as e:
            print(f"  - {date_str}: エラー {e}")
        
        d -= timedelta(days=1)
    
    raise RuntimeError(f"直近 {lookback_days} 日間に有効なデータが見つかりませんでした。")

def _fetch_all_issues_for_date(api_key: str, date_str: str) -> list[dict]:
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
        time.sleep(0.3)
    return rows

# ---------------------------
# メインロジック
# ---------------------------
def sync_data():
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    db = database_manager.DBManager()
    
    print("🚀 データ同期エンジン起動")

    # 1. Bulk API への挑戦
    try:
        files = _bulk_list_files(api_key)
        if files:
            print(f"📦 Bulkルート: {len(files)} ファイルを検出")
            for f in sorted(files, key=lambda x: x.get("Key", "")):
                url = _bulk_get_download_url(api_key, f["Key"])
                for chunk in _read_csv_gz_from_url(url):
                    db.save_prices(_normalize_df(chunk))
            print("🎉 Bulk同期完了")
            return
    except Exception as e:
        print(f"⚠️ Bulk利用不可 (ステータス: {e})。Daily APIへ切り替えます。")

    # 2. Daily API による地道なバックフィル
    latest_date_str = _find_latest_trading_date_str(api_key)
    current_d = pd.to_datetime(latest_date_str).date()
    
    print(f"📅 Dailyルート: {latest_date_str} から過去へ遡ります")

    # Actionsの制限時間を考慮し、とりあえず直近30日分を目指す（調整可能）
    for _ in range(30):
        d_str = current_d.strftime("%Y-%m-%d")
        print(f"📥 {d_str} の全銘柄データを取得中...")
        
        try:
            rows = _fetch_all_issues_for_date(api_key, d_str)
            if rows:
                df = pd.DataFrame(rows)
                df["date"] = pd.to_datetime(df["Date"]).dt.date
                df["ticker"] = df["Code"].astype(str)
                
                col_map = {"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"}
                df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
                
                target_cols = ["ticker", "date", "open", "high", "low", "price", "volume"]
                db.save_prices(df[target_cols].drop_duplicates())
                print(f"✅ {len(df)} 件保存完了")
            else:
                print(f"⚠️ {d_str}: データがありませんでした")
        except Exception as e:
            if "400" in str(e):
                print(f"⛔ {d_str}: 取得限界に達した可能性があります。")
                break
            print(f"❌ {d_str} でエラー: {e}")

        current_d -= timedelta(days=1)
        # 土日を飛ばす
        while current_d.weekday() >= 5:
            current_d -= timedelta(days=1)
        time.sleep(1)

    print("✨ 全工程終了")

if __name__ == "__main__":
    sync_data()
