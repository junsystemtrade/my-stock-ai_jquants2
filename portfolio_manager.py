import os
import requests
import pandas as pd
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()

    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    base_url = "https://api.jquants.com/v2/equities/bars/daily"
    headers = {"x-api-key": api_key, "Accept": "application/json"}

    # 銘柄: 30480 (ビックカメラ)
    code = "30480"
    params = {"code": code}

    print(f"🎯 J-Quants V2 データ抽出開始: {code}")

    try:
        res = requests.get(base_url, headers=headers, params=params, timeout=30)
        res.raise_for_status()

        quotes = res.json().get("data", [])
        if not quotes:
            print("⚠️ データが取得できませんでした")
            return

        df = pd.DataFrame(quotes)
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df["ticker"] = code

        COLUMN_MAP = {"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"}
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

        cols = ["ticker", "date", "open", "high", "low", "price", "volume"]
        df = df[cols].drop_duplicates(subset=["ticker", "date"]).sort_values("date")

        db.save_prices(df)
        print(f"✨【完遂】{code} の {len(df)} 件を同期しました。")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
        raise

if __name__ == "__main__":
    sync_data()
