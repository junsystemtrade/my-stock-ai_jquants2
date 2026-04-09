"""
portfolio_manager.py
====================
株価データの取得（yfinance）および DB への保存を担当。
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
_YF_SLEEP = float(os.getenv("YF_SLEEP_SEC", "3.0"))
MARKET_TICKER = "NIY=F"  # 日経平均先物(CME)

def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()

def _to_yf_ticker(code):
    if "=" in code or "^" in code:
        return code
    code_only = str(code).replace(".T", "").strip()
    return f"{code_only[:4]}.T" if len(code_only) >= 4 else code_only

# -----------------------------------------------------------------------
# 銘柄マスター取得
# -----------------------------------------------------------------------
def get_target_tickers():
    """JPXから最新の銘柄リストを取得。"""
    base_url = "https://www.jpx.co.jp"
    list_page = f"{base_url}/markets/statistics-equities/misc/01.html"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(list_page, headers=headers, timeout=20)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, "html.parser")
        link = soup.find("a", href=lambda x: x and "data_j.xls" in x)
        
        if link:
            excel_url = base_url + link["href"]
            resp = requests.get(excel_url, headers=headers, timeout=30)
            df = pd.read_excel(io.BytesIO(resp.content))
            
            stock_map = {}
            for _, row in df.iterrows():
                code = str(row.iloc[1]).strip()
                name = str(row.iloc[2]).strip()
                if code.isdigit() and len(code) >= 4:
                    stock_map[f"{code[:4]}.T"] = {"name": name}
            
            print(f"✅ JPXマスター取得完了: {len(stock_map)} 銘柄")
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
        raw = yf.download(tickers, start=start, end=end, interval="1d", 
                          auto_adjust=True, progress=False, group_by="ticker", threads=True)
    except Exception as e:
        print(f" ❌ Error: {e}")
        return pd.DataFrame()
    
    if raw.empty:
        print(" ⚠️ No Data")
        return pd.DataFrame()

    all_dfs = []
    fetched_tickers = tickers if len(tickers) == 1 else raw.columns.levels[0]

    for t in fetched_tickers:
        try:
            df_t = raw[t].copy() if len(tickers) > 1 else raw.copy()
            if df_t.empty or "Close" not in df_t.columns:
                continue
                
            df_t["ticker"] = t
            df_t["date"] = df_t.index.date
            df_t["price"] = df_t["Close"]
            df_t = df_t.rename(columns={"Open": "open", "High": "high", "Low": "low", "Volume": "volume"})
            
            valid_df = df_t[["ticker", "date", "open", "high", "low", "price", "volume"]].dropna(subset=["price"])
            if not valid_df.empty:
                all_dfs.append(valid_df)
        except:
            continue

    print(" Done")
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

# -----------------------------------------------------------------------
# 同期・バックフィル
# -----------------------------------------------------------------------
def _prev_business_day(d):
    d -= timedelta(days=1)
    while d.weekday() >= 5: d -= timedelta(days=1)
    return d

def sync_data():
    """
    日次の差分更新：
    バックフィル進捗に関わらず、直近7日分のデータを取得し、
    NIY=FおよびDBに存在する銘柄を最新に保つ。
    """
    db = database_manager.DBManager()
    target_end = _prev_business_day(_today_jst())
    
    # 銘柄リスト準備
    target_stock_map = get_target_tickers()
    all_tickers = list(target_stock_map.keys())
    if MARKET_TICKER not in all_tickers:
        all_tickers.append(MARKET_TICKER)

    # 4,000銘柄を一括リクエストすると重いため、
    # 「すでにDBにある銘柄」+「NIY=F」に絞り込む
    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            existing = {row[0] for row in conn.execute(text("SELECT ticker FROM daily_prices GROUP BY ticker"))}
    except:
        existing = set()
    
    # NIY=F はDBになくても必ず取得対象にする
    sync_targets = [t for t in all_tickers if t in existing or t == MARKET_TICKER]

    print(f"🔄 同期開始 (ターゲット日: {target_end})")
    print(f"📦 対象銘柄数: {len(sync_targets)}")

    # 直近7日分をガバッと取って「重複スキップ(DB側)」に任せる
    start_date = target_end - timedelta(days=7)
    
    # 銘柄数が多い可能性があるため、同期も念のためチャンク分割(100銘柄ずつ)
    chunk_size = 100
    for i in range(0, len(sync_targets), chunk_size):
        chunk = sync_targets[i : i + chunk_size]
        df = _yf_fetch_chunk(chunk, start_date.strftime("%Y-%m-%d"), 
                             (target_end + timedelta(days=1)).strftime("%Y-%m-%d"))
        
        if not df.empty:
            db.save_prices(df)
            
    print(f"✨ 同期処理が終了しました（NIY=F含む）")

def backfill_data():
    """過去数年分のデータを一括取得（初回用）"""
    db = database_manager.DBManager()
    today = _today_jst()
    start_str = (today - timedelta(days=BACKFILL_YEARS * 365)).strftime("%Y-%m-%d")
    end_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    tickers = list(get_target_tickers().keys())
    if MARKET_TICKER not in tickers:
        tickers.append(MARKET_TICKER)

    from sqlalchemy import text
    try:
        with db.engine.connect() as conn:
            existing = {row[0] for row in conn.execute(text("SELECT ticker FROM daily_prices GROUP BY ticker"))}
    except:
        existing = set()

    remaining = [t for t in tickers if t not in existing]
    
    if not remaining:
        print("✅ 全銘柄のバックフィルが完了しています。")
        return

    print(f"🚀 バックフィル開始: 残り {len(remaining)} 銘柄")

    chunk_size = 50
    for i in range(0, len(remaining), chunk_size):
        chunk = remaining[i : i + chunk_size]
        print(f"\n--- Batch {i//chunk_size + 1} / {(len(remaining)-1)//chunk_size + 1} ---")
        df_chunk = _yf_fetch_chunk(chunk, start_str, end_str)
        
        if not df_chunk.empty:
            db.save_prices(df_chunk)
        
        time.sleep(_YF_SLEEP)

    print(f"\n✨ 全バックフィル工程が正常に終了しました。")

if __name__ == "__main__":
    db = database_manager.DBManager()
    
    # 1. ターゲットとなる全銘柄リストを取得
    target_stocks = get_target_tickers()
    all_target_tickers = list(target_stocks.keys())
    if MARKET_TICKER not in all_target_tickers:
        all_target_tickers.append(MARKET_TICKER)

    # 2. 現在DBに存在する銘柄の「種類」を取得
    from sqlalchemy import text
    existing_tickers = set()
    try:
        with db.engine.connect() as conn:
            # 銘柄ごとの最新1件だけ見ればいいので、DISTINCT または GROUP BY で高速に取得
            res = conn.execute(text("SELECT ticker FROM daily_prices GROUP BY ticker"))
            existing_tickers = {row[0] for row in res}
    except Exception as e:
        print(f"⚠️ DB接続エラー: {e}")

    # 3. 未取得の銘柄があるか判定
    # targetにはあるが、DBにはまだ1件もデータがない銘柄を抽出
    remaining_tickers = [t for t in all_target_tickers if t not in existing_tickers]

    print(f"📊 進捗確認: 全 {len(all_target_tickers)} 銘柄中、{len(existing_tickers)} 銘柄がDBに存在します。")

    if len(remaining_tickers) > 0:
        # 未完了の銘柄が1つでもあるならバックフィルモードを継続
        print(f"📝 残り {len(remaining_tickers)} 銘柄のバックフィルを開始します...")
        backfill_data()
    else:
        # 全銘柄の「初回の過去分」が揃っていれば、日次同期モードに切り替え
        print("✅ 全銘柄のバックフィルが完了しています。同期モード(sync_data)を実行します。")
        sync_data()
