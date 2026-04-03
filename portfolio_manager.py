# -*- coding: utf-8 -*-

# “””
portfolio_manager.py

株価データの取得/DB 同期を担当するモジュール。

【設計方針】
Yahoo Finance を主軸に全銘柄/全期間を確実に取得する。

問題: 一括取得（全銘柄×全期間）では上場廃止銘柄のエラーが多発し
多くの銘柄がスキップされる。

解決策: 銘柄ごとに個別取得する。
- 1銘柄ずつ取得するので上場廃止でもスキップせず次に進む
- チェックポイント方式で途中停止→再開が可能
- 取得済み銘柄はスキップするので重複取得しない

【所要時間の目安】
4000銘柄 × 0.5秒/銘柄 = 約33分（3年分）
GitHub Actions 6時間以内に完了可能
“””

import io
import os
import time
import requests
import pandas as pd
import yfinance as yf

from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

import database_manager

# ———————————————————————–

# 定数

# ———————————————————————–

BACKFILL_YEARS = 3

# 1銘柄ずつ取得する際の待機秒数（Yahoo Finance への負荷対策）

_YF_SINGLE_SLEEP = float(os.getenv(“YF_SINGLE_SLEEP”, “0.3”))

# チャンク取得の銘柄数（差分取得用）

_YF_CHUNK_SIZE = int(os.getenv(“YF_CHUNK_SIZE”, “50”))
_YF_SLEEP      = float(os.getenv(“YF_SLEEP_SEC”, “3.0”))

# DB INSERT の分割単位

_DB_CHUNK_SIZE = int(os.getenv(“DB_CHUNK_SIZE”, “1000”))

# J-Quants（最新日確認のみ）

BASE_API          = “https://api.jquants.com/v2”
EQ_DAILY_ENDPOINT = “/equities/bars/daily”
_SAMPLE_CODES     = [“72030”, “86580”, “90840”, “30480”]
_JQUANTS_INTERVAL = float(os.getenv(“JQUANTS_MIN_INTERVAL_SEC”, “12.5”))

# ———————————————————————–

# JST 今日の日付

# ———————————————————————–

def _today_jst() -> date:
return datetime.now(ZoneInfo(“Asia/Tokyo”)).date()

# ———————————————————————–

# 銘柄コードユーティリティ

# ———————————————————————–

def _to_yf_ticker(code: str) -> str:
code_only = code.replace(”.T”, “”).strip()
if len(code_only) > 4:
code_only = code_only[:4]
return f”{code_only}.T”

def _to_db_ticker(yf_ticker: str) -> str:
return _to_yf_ticker(yf_ticker)

# ———————————————————————–

# JPX 上場銘柄マスタ取得

# ———————————————————————–

def get_target_tickers() -> dict:
try:
import jpx_master
raw = jpx_master.get_target_tickers()
return {_to_yf_ticker(k): v for k, v in raw.items()}
except ImportError:
pass

```
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
```

# ———————————————————————–

# J-Quants: 最新日確認のみ

# ———————————————————————–

def _jq_latest_date() -> date | None:
api_key = os.getenv(“JQUANTS_API_KEY”, “”).strip()
if not api_key:
return None

```
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
        print(f"  📌 J-Quants code={code}: 最新日 {d}")
        if best is None or d > best:
            best = d
    except Exception:
        continue

return best
```

# ———————————————————————–

# Yahoo Finance 1銘柄取得

# ———————————————————————–

def _yf_fetch_single(ticker: str, start: str, end: str) -> pd.DataFrame:
“””
1銘柄の株価を Yahoo Finance から取得する。
上場廃止でも例外を投げずに空 DataFrame を返す。
“””
try:
raw = yf.download(
ticker,
start=start,
end=end,
interval=“1d”,
auto_adjust=True,
progress=False,
timeout=30,
)
except Exception:
return pd.DataFrame()

```
if raw is None or raw.empty:
    return pd.DataFrame()

# MultiIndex 対応
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
```

# ———————————————————————–

# Yahoo Finance チャンク取得（差分用）

# ———————————————————————–

def _yf_fetch_chunk(tickers: list[str], start: str, end: str) -> pd.DataFrame:
“”“複数銘柄を一括取得（差分取得の高速化用）。”””
if not tickers:
return pd.DataFrame()

```
all_records = []

for i in range(0, len(tickers), _YF_CHUNK_SIZE):
    chunk = tickers[i : i + _YF_CHUNK_SIZE]
    chunk_no = i // _YF_CHUNK_SIZE + 1
    n_chunks = (len(tickers) + _YF_CHUNK_SIZE - 1) // _YF_CHUNK_SIZE
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

return (
    pd.DataFrame(all_records)
    .drop_duplicates(subset=["ticker", "date"])
    .reset_index(drop=True)
)
```

# ———————————————————————–

# 前営業日を返す

# ———————————————————————–

def _prev_business_day(d: date) -> date:
d -= timedelta(days=1)
while d.weekday() >= 5:
d -= timedelta(days=1)
return d

# ———————————————————————–

# 通常差分同期（daily_scan.yml から呼ばれる）

# ———————————————————————–

def sync_data():
“”“DB の最終日の翌日〜今日を差分取得。チャンク一括取得で高速。”””
db = database_manager.DBManager()

```
print("🔍 J-Quants 最新日を確認中（参考情報）...")
jq_latest = _jq_latest_date()
if jq_latest:
    print(f"✅ J-Quants 最新日: {jq_latest}")

target_end    = _prev_business_day(_today_jst())
db_latest_str = db.get_latest_saved_date()

if db_latest_str is None:
    print("⚠️ DBにデータがありません。Initial Backfill を先に実行してください。")
    return

db_latest  = date.fromisoformat(db_latest_str)
start_date = db_latest + timedelta(days=1)

if start_date > target_end:
    print(f"✅ DB はすでに最新です（最終日: {db_latest}）。スキップします。")
    return

print(f"🔄 差分取得: {start_date} 〜 {target_end}")

ticker_map     = get_target_tickers()
all_yf_tickers = list(ticker_map.keys())
start_str      = start_date.strftime("%Y-%m-%d")
end_str        = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

print(f"📡 Yahoo Finance 一括取得: {len(all_yf_tickers)} 銘柄")
df = _yf_fetch_chunk(all_yf_tickers, start_str, end_str)

if df.empty:
    print("⚠️ データが取得できませんでした。")
    return

db.save_prices(df)
print(f"\n🎉 差分同期完了: {len(df):,} 件保存")
```

# ———————————————————————–

# バックフィル専用（Initial Backfill workflow から呼ばれる）

# ———————————————————————–

def backfill_data():
“””
全銘柄を1銘柄ずつ取得して3年分のデータを確実に蓄積する。

```
【なぜ1銘柄ずつか】
一括取得では上場廃止銘柄のエラーで多くの銘柄がスキップされる。
1銘柄ずつ取得することで確実にデータを取得できる。

【チェックポイント方式】
取得済み銘柄数をカウントして途中停止→再開が可能。
DBに既にデータがある銘柄はスキップする。

【所要時間】
4000銘柄 × 0.3秒 = 約20分（3年分を一度に取得）
"""
db = database_manager.DBManager()

today        = _today_jst()
target_start = today - timedelta(days=BACKFILL_YEARS * 365)
target_end   = _prev_business_day(today)
start_str    = target_start.strftime("%Y-%m-%d")
end_str      = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

# DB 状況確認
db_oldest_str = db.get_oldest_saved_date()
db_latest_str = db.get_latest_saved_date()

print(f"\n📊 DB 現在の状況:")
print(f"   最古日: {db_oldest_str or 'なし'}")
print(f"   最終日: {db_latest_str or 'なし'}")
print(f"   目標期間: {target_start} 〜 {target_end}")

# 銘柄マスタ取得
ticker_map     = get_target_tickers()
all_yf_tickers = list(ticker_map.keys())
total          = len(all_yf_tickers)

print(f"\n🚀 バックフィル開始: {total} 銘柄 × {BACKFILL_YEARS}年分")
print(f"   取得方式: 1銘柄ずつ（確実取得モード）")
print(f"   所要時間目安: 約{total * _YF_SINGLE_SLEEP / 60:.0f}〜{total * _YF_SINGLE_SLEEP * 2 / 60:.0f}分")
print(f"   ✅ 途中停止しても次回実行時に続きから再開します\n")

# DB に既に入っている銘柄のリストを取得（スキップ用）
existing_tickers = _get_existing_tickers(db, target_start)
print(f"   既取得済み: {len(existing_tickers)} 銘柄（スキップ）")
print(f"   取得対象  : {total - len(existing_tickers)} 銘柄\n")

saved_total  = 0
skip_count   = 0
error_count  = 0
batch_buffer = []
batch_size   = 100  # 100銘柄分まとめてDB保存

for idx, ticker in enumerate(all_yf_tickers, 1):
    # 既に十分なデータがある銘柄はスキップ
    if ticker in existing_tickers:
        skip_count += 1
        continue

    if idx % 100 == 0 or idx == 1:
        print(f"  [{idx}/{total}] 処理中... 保存済み: {saved_total:,}件", flush=True)

    df = _yf_fetch_single(ticker, start_str, end_str)

    if df.empty:
        error_count += 1
    else:
        batch_buffer.append(df)

    # バッチ保存
    if len(batch_buffer) >= batch_size:
        combined     = pd.concat(batch_buffer, ignore_index=True)
        db.save_prices(combined)
        saved_total  += len(combined)
        batch_buffer  = []

    time.sleep(_YF_SINGLE_SLEEP)

# 残りを保存
if batch_buffer:
    combined     = pd.concat(batch_buffer, ignore_index=True)
    db.save_prices(combined)
    saved_total += len(combined)

# 完了確認
new_oldest = db.get_oldest_saved_date()
new_latest = db.get_latest_saved_date()

# DBの実際のデータ量を確認
total_rows = _count_rows(db)

print(f"\n{'='*40}")
print(f"🎉 バックフィル完了")
print(f"   保存件数  : {saved_total:,} 件")
print(f"   スキップ  : {skip_count} 銘柄（既取得済み）")
print(f"   取得失敗  : {error_count} 銘柄（上場廃止等）")
print(f"   DB総件数  : {total_rows:,} 件")
print(f"   DB最古日  : {new_oldest}")
print(f"   DB最終日  : {new_latest}")

if new_oldest and date.fromisoformat(new_oldest) <= target_start + timedelta(days=10):
    print(f"   ✅ {BACKFILL_YEARS}年分のデータが揃いました！")
else:
    print(f"   ⏳ まだ途中です。再度 Initial Backfill を実行してください。")
print(f"{'='*40}")
```

def _get_existing_tickers(db: database_manager.DBManager, since: date) -> set:
“””
指定日以降のデータが既に十分ある銘柄のセットを返す（スキップ判定用）。
50日以上データがある銘柄は取得済みとみなす。
“””
from sqlalchemy import text
query = text(”””
SELECT ticker
FROM daily_prices
WHERE date >= :since
GROUP BY ticker
HAVING COUNT(*) >= 50
“””)
try:
with db.engine.connect() as conn:
result = conn.execute(query, {“since”: str(since)})
return {row[0] for row in result}
except Exception:
return set()

def _count_rows(db: database_manager.DBManager) -> int:
“”“DB の総件数を返す。”””
from sqlalchemy import text
try:
with db.engine.connect() as conn:
result = conn.execute(text(“SELECT COUNT(*) FROM daily_prices”))
return result.fetchone()[0]
except Exception:
return 0

# ———————————————————————–

# エントリーポイント

# ———————————————————————–

if **name** == “**main**”:
backfill_data()