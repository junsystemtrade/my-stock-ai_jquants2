import os
import requests
import pandas as pd
from datetime import date, timedelta
import database_manager

def sync_data():
    db = database_manager.DBManager()
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()

    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    base_url = "https://api.jquants.com/v2/equities/bars/daily"
    headers = {"x-api-key": api_key, "Accept": "application/json"}

    # 🎯 J-Quants 正式コード（5桁）とテスト用日付
    code = "30480"
    target_date = "20251201" # 運用時は (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    print(f"🎯 J-Quants V2 データ抽出開始: {code}")

    try:
        res = requests.get(base_url, headers=headers, params={"code": code, "date": target_date}, timeout=20)
        res.raise_for_status()

        raw_data = res.json()
        quotes = raw_data.get("data", []) # V2は 'data' キー

        if not quotes:
            print(f"⚠️ データが空でした: {raw_data}")
            return

        df = pd.DataFrame(quotes)

        # カラム名変換（V2の揺れをすべて吸収）
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df["ticker"] = code

        COLUMN_MAP = {
            "O": "open", "H": "high", "L": "low", "Low": "low", "C": "price", "Vo": "volume"
        }
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

        # 必須カラムの存在チェック
        required_cols = ["ticker", "date", "open", "high", "low", "price", "volume"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"必要なカラムが不足しています: {missing}")

        # 重複排除して保存
        df = df.drop_duplicates(subset=["ticker", "date"])
        db.save_prices(df[required_cols])
        
        print("✨【完遂】J-Quants から Supabase への同期が完了しました")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
        raise
