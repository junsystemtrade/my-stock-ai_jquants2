"""
portfolio_manager.py

株価データの取得（yfinance）および DB への保存を担当。

変更点:
- sync_data() の差分判定を修正
  「直近7日以内にデータがある」ではなく
  「DBの最終日 >= 前営業日」の銘柄をスキップに変更
  → 毎日確実に最新データを取得できるようになる
- sync_market_ticker() で NIY=F を毎回必ず取得
- backfill_data() の除外条件を「データが存在する」→「min_data_days 以上ある」に変更
  → 新規上場銘柄（DBデータが少ない銘柄）も月次バックフィルで自動補完される
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

BACKFILL_YEARS = 3
_YF_SLEEP      = float(os.getenv("YF_SLEEP_SEC", "3.0"))
MARKET_TICKER  = "NIY=F"

def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()

def _prev_business_day(d):
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

# -------------------------------------------------------------------------
# 銘柄マスター取得
# -------------------------------------------------------------------------

def get_target_tickers():
    """JPXから最新の銘柄リストを取得。整理・監理ポスト銘柄を除外。"""
    base_url  = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers   = {"User-Agent": "Mozilla/5.0"}

    try:
        res  = requests.get(list_page, headers=headers, timeout=20)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)

        if link:
            excel_url = base_url + link["href"]
            resp      = requests.get(excel_url, headers=headers, timeout=30)

            if excel_url.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
            else:
                df = pd.read_excel(io.BytesIO(resp.content), engine="xlrd")

            EXCLUDE_MARKETS = [
                "整理（内国株式）",
                "監理（内国株式）",
                "整理（外国株式）",
                "監理（外国株式）",
            ]
            market_col = "市場・商品区分"
            if market_col in df.columns:
                before   = len(df)
                df       = df[~df[market_col].isin(EXCLUDE_MARKETS)]
                excluded = before - len(df)
                if excluded > 0:
                    print(f"⛔ 整理・監理ポスト除外: {excluded} 銘柄")

            stock_map = {}
            for _, row in df.iterrows():
                code = str(row.iloc[1]).strip()
                name = str(row.iloc[2]).strip()
                if code.isdigit() and len(code) >= 4:
                    stock_map[f"{code[:4]}.T"] = {"name": name}

            print(f"✅ JPXマスター取得完了: {len(stock_map)} 銘柄（整理・監理ポスト除外済み）")
            return stock_map

    except Exception as e:
        print(f"⚠️ JPXマスター取得エラー (サンプルを使用します): {e}")

    return {"7203.T": {"name": "トヨタ"}, "8306.T": {"name": "三菱UFJ"}}

# -------------------------------------------------------------------------
# データダウンロード
# -------------------------------------------------------------------------

def _yf_fetch_chunk(tickers, start, end):
    """複数銘柄を一括ダウンロードして整形"""
    if not tickers:
        return pd.DataFrame()

    print(f" 📥 Download {len(tickers)} tickers ({start}->{end})...", end="", flush=True)
    try:
        raw = yf.download(
            tickers,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
        )
    except Exception as e:
        print(f" Error: {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        print(" No Data")
        return pd.DataFrame()

    all_dfs         = []
    fetched_tickers = tickers if len(tickers) == 1 else (
        raw.columns.levels[0]
        if isinstance(raw.columns, pd.MultiIndex)
        else tickers
    )

    for t in fetched_tickers:
        try:
            df_t = raw[t].copy() if len(tickers) > 1 else raw.copy()
            if df_t.empty or "Close" not in df_t.columns:
                continue
            df_t["ticker"] = t
            df_t["date"]   = df_t.index.date
            df_t["price"]  = df_t["Close"]
            df_t = df_t.rename(columns={
                "Open": "open", "High": "high",
                "Low": "low", "Volume": "volume"
            })
            valid = df_t[
                ["ticker", "date", "open", "high", "low", "price", "volume"]
            ].dropna(subset=["price"])
            if not valid.empty:
                all_dfs.append(valid)
        except Exception:
            continue

    print(" Done")
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def _yf_fetch_single(ticker, start, end):
    """1銘柄取得（NIY=F等の特殊シンボル用）"""
    try:
        raw = yf.download(
            ticker, start=start, end=end,
            interval="1d", auto_adjust=True, progress=False,
        )
    except Exception as e:
        print(f" ⚠️ {ticker} 取得失敗: {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    raw["ticker"] = ticker
    raw["date"]   = raw.index.date
    raw["price"]  = raw["Close"]
    raw = raw.rename(columns={
        "Open": "open", "High": "high",
        "Low": "low", "Volume": "volume"
    })
    valid = raw[
        ["ticker", "date", "open", "high", "low", "price", "volume"]
    ].dropna(subset=["price"])
    return valid.reset_index(drop=True)

# -------------------------------------------------------------------------
# NIY=F の毎日同期
# -------------------------------------------------------------------------

def sync_market_ticker(db):
    """NIY=F を毎回必ず直近10日分取得してDB保存する"""
    today     = _today_jst()
    start_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    end_str   = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📡 NIY=F 同期中...", end="", flush=True)
    df = _yf_fetch_single(MARKET_TICKER, start_str, end_str)

    if not df.empty:
        db.save_prices(df)
        print(f" ✅ {len(df)} 件保存")
    else:
        print(" ⚠️ 取得失敗")

# -------------------------------------------------------------------------
# 通常差分同期
# -------------------------------------------------------------------------

def sync_data():
    """
    日次の差分更新。
    各銘柄のDB最終日 >= 前営業日の銘柄はスキップし、
    前営業日のデータがまだない銘柄だけ取得することで毎日確実に最新データを入れる。
    """
    db            = database_manager.DBManager()
    target_end    = _prev_business_day(_today_jst())
    target_end_str = str(target_end)

    # ① NIY=F を必ず同期
    sync_market_ticker(db)

    # ② 各銘柄の最終日を取得して、前営業日のデータがある銘柄をスキップ
    target_stock_map = get_target_tickers()
    all_tickers      = list(target_stock_map.keys())

    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT ticker, MAX(date) as latest_date
                FROM daily_prices
                WHERE ticker != 'NIY=F'
                GROUP BY ticker
            """))
            latest_dates = {row[0]: str(row[1]) for row in result}
    except Exception:
        latest_dates = {}

    # 前営業日のデータがまだない銘柄のみ取得対象
    sync_targets = [
        t for t in all_tickers
        if latest_dates.get(t, "1900-01-01") < target_end_str
    ]

    if not sync_targets:
        print(f"✅ 全銘柄は最新状態です（最終日: {target_end}）。通常同期をスキップします。")
        return

    print(f"🔄 差分同期開始 (前営業日: {target_end})")
    print(f"📦 未更新銘柄数: {len(sync_targets)} / {len(all_tickers)}")

    # 取得範囲は前営業日から5日前まで（祝日対応）
    start_str  = (target_end - timedelta(days=5)).strftime("%Y-%m-%d")
    end_str    = (target_end + timedelta(days=1)).strftime("%Y-%m-%d")

    chunk_size = 100
    for i in range(0, len(sync_targets), chunk_size):
        chunk = sync_targets[i: i + chunk_size]
        df    = _yf_fetch_chunk(chunk, start_str, end_str)
        if not df.empty:
            db.save_prices(df)

    print("✨ 差分同期完了")

# -------------------------------------------------------------------------
# バックフィル
# -------------------------------------------------------------------------

def backfill_data():
    """
    過去数年分のデータを一括取得（初回・追加用）。

    除外条件を「DBに存在するか」から「min_data_days 以上データがあるか」に変更。
    → 新規上場銘柄（DBデータが少ない銘柄）も月次バックフィルで自動補完される。
    """
    db        = database_manager.DBManager()
    today     = _today_jst()
    start_str = (today - timedelta(days=BACKFILL_YEARS * 365)).strftime("%Y-%m-%d")
    end_str   = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    # signals_config.yml から min_data_days を取得
    try:
        from signal_engine import _load_config
        cfg          = _load_config()
        min_data_days = cfg["filter"]["min_data_days"]
    except Exception:
        min_data_days = 80
    print(f"📋 min_data_days: {min_data_days}（これ未満の銘柄はバックフィル対象）")

    tickers = list(get_target_tickers().keys())
    if MARKET_TICKER not in tickers:
        tickers.append(MARKET_TICKER)

    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            # 銘柄ごとのデータ件数を取得
            result = conn.execute(text("""
                SELECT ticker, COUNT(*) as data_count
                FROM daily_prices
                GROUP BY ticker
            """))
            data_counts = {row[0]: row[1] for row in result}
    except Exception:
        data_counts = {}

    # min_data_days 未満の銘柄のみバックフィル対象
    # （新規上場銘柄・データ不足銘柄を自動補完）
    remaining = [
        t for t in tickers
        if data_counts.get(t, 0) < min_data_days
    ]

    if not remaining:
        print(f"✅ 全銘柄のデータが {min_data_days} 件以上あります。バックフィルをスキップします。")
        return

    print(f"🚀 バックフィル開始: 対象 {len(remaining)} 銘柄 / 全 {len(tickers)} 銘柄")
    print(f"   （うち新規上場・データ不足: {sum(1 for t in remaining if data_counts.get(t, 0) > 0)} 銘柄が部分データあり）")

    chunk_size = 20
    for i in range(0, len(remaining), chunk_size):
        chunk = remaining[i: i + chunk_size]
        print(f"\n--- Batch {i//chunk_size + 1} / {(len(remaining)-1)//chunk_size + 1} ---")
        df_chunk = _yf_fetch_chunk(chunk, start_str, end_str)
        if not df_chunk.empty:
            db.save_prices(df_chunk)
        time.sleep(_YF_SLEEP)

    print("\n✨ 全バックフィル工程が正常に終了しました。")

if __name__ == "__main__":
    backfill_data()
