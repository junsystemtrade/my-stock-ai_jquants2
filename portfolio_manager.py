"""
portfolio_manager.py
====================
株価データの取得（yfinance）および DB への保存を担当。

変更点:
  - sync_data() で NIY=F を毎回必ず取得・DB保存するよう修正
  - NIY=F は already_updated チェックの対象外にする
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
# 設定定数
# -----------------------------------------------------------------------
BACKFILL_YEARS = 3
_YF_SLEEP      = float(os.getenv("YF_SLEEP_SEC", "3.0"))
MARKET_TICKER  = "NIY=F"


def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


def _to_yf_ticker(code):
    if "=" in code or "^" in code:
        return code
    code_only = str(code).replace(".T", "").strip()
    return f"{code_only[:4]}.T" if len(code_only) >= 4 else code_only


def _prev_business_day(d):
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


# -----------------------------------------------------------------------
# 銘柄マスター取得
# -----------------------------------------------------------------------
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
            df        = pd.read_excel(io.BytesIO(resp.content))

            # 整理・監理ポスト除外
            EXCLUDE_MARKETS = [
                "整理（内国株式）",
                "監理（内国株式）",
                "整理（外国株式）",
                "監理（外国株式）",
            ]
            market_col = "市場・商品区分"
            if market_col in df.columns:
                before = len(df)
                df     = df[~df[market_col].isin(EXCLUDE_MARKETS)]
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


# -----------------------------------------------------------------------
# データダウンロード
# -----------------------------------------------------------------------
def _yf_fetch_chunk(tickers, start, end):
    """複数銘柄を一括ダウンロードして整形"""
    if not tickers:
        return pd.DataFrame()

    print(f" 📥 Download {len(tickers)} tickers...", end="", flush=True)
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

            valid_df = df_t[
                ["ticker", "date", "open", "high", "low", "price", "volume"]
            ].dropna(subset=["price"])
            if not valid_df.empty:
                all_dfs.append(valid_df)
        except Exception:
            continue

    print(" Done")
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def _yf_fetch_single(ticker, start, end):
    """1銘柄を取得して整形（NIY=F 等の特殊シンボル用）"""
    try:
        raw = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,
            progress=False,
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


# -----------------------------------------------------------------------
# NIY=F の毎日同期（デイリースキャン用）
# -----------------------------------------------------------------------
def sync_market_ticker(db):
    """
    NIY=F（日経平均先物）を毎日必ずDBに保存する。
    already_updated チェックの対象外とし、常に最新7日分を取得する。
    """
    today     = _today_jst()
    start_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    end_str   = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📡 NIY=F 同期中 ({start_str} -> {today})...", end="", flush=True)
    df = _yf_fetch_single(MARKET_TICKER, start_str, end_str)

    if not df.empty:
        db.save_prices(df)
        print(f"✅ NIY=F 保存完了: {len(df)} 件")
    else:
        print("⚠️ NIY=F データ取得失敗")


# -----------------------------------------------------------------------
# 通常差分同期（daily_scan.yml から呼ばれる）
# -----------------------------------------------------------------------
def sync_data():
    """
    日次の差分更新:
    1. NIY=F を必ず最新化（地合い判定用）
    2. DBに既存の銘柄を直近7日分で差分取得
    """
    db         = database_manager.DBManager()
    target_end = _prev_business_day(_today_jst())
    target_start = target_end - timedelta(days=7)

    # ① NIY=F を必ず同期（already_updated チェック不要）
    sync_market_ticker(db)

    # ② 通常銘柄: 直近7日以内にデータが存在する銘柄は最新とみなしてスキップ
    target_stock_map = get_target_tickers()
    all_tickers      = list(target_stock_map.keys())

    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            # NIY=F 以外で直近7日以内にデータがある銘柄 = 更新済みとみなす
            result = conn.execute(text("""
                SELECT DISTINCT ticker
                FROM daily_prices
                WHERE date >= :since
                  AND ticker != 'NIY=F'
            """), {"since": str(target_start)})
            already_updated = {row[0] for row in result}
    except Exception:
        already_updated = set()

    # 未更新の銘柄のみ対象
    sync_targets = [t for t in all_tickers if t not in already_updated]

    if not sync_targets:
        print("✅ 全銘柄は最新状態です。通常同期をスキップします。")
        return

    print(f"🔄 差分同期開始 (ターゲット日: {target_end})")
    print(f"📦 未更新銘柄数: {len(sync_targets)}")

    chunk_size = 100
    for i in range(0, len(sync_targets), chunk_size):
        chunk = sync_targets[i: i + chunk_size]
        df    = _yf_fetch_chunk(
            chunk,
            target_start.strftime("%Y-%m-%d"),
            (target_end + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
        if not df.empty:
            db.save_prices(df)

    print("✨ 差分同期完了")


# -----------------------------------------------------------------------
# バックフィル（Initial Backfill workflow から呼ばれる）
# -----------------------------------------------------------------------
def backfill_data():
    """過去数年分のデータを一括取得（初回・追加用）"""
    db        = database_manager.DBManager()
    today     = _today_jst()
    start_str = (today - timedelta(days=BACKFILL_YEARS * 365)).strftime("%Y-%m-%d")
    end_str   = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    tickers = list(get_target_tickers().keys())
    if MARKET_TICKER not in tickers:
        tickers.append(MARKET_TICKER)

    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            existing = {
                row[0] for row in conn.execute(
                    text("SELECT ticker FROM daily_prices GROUP BY ticker")
                )
            }
    except Exception:
        existing = set()

    remaining = [t for t in tickers if t not in existing]

    if not remaining:
        print("✅ 全銘柄のバックフィルが完了しています。")
        return

    print(f"🚀 バックフィル開始: 残り {len(remaining)} 銘柄")

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
