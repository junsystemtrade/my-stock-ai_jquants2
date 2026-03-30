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
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    # ✅ J-Quants 正式コード（5桁）
    code = "30480"

    # ✅ 前営業日相当（最低限の安全策）
    target_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    params = {
        "code": code,
        "date": target_date,
    }

    print(f"🎯 J-Quants V2 データ抽出開始: {code} / {target_date}")

    try:
        res = requests.get(
            base_url,
            headers=headers,
            params=params,
            timeout=20,
        )
        res.raise_for_status()

        raw_data = res.json()
        quotes = raw_data.get("data", [])

        if not quotes:
            print(f"⚠️ データが空でした: {raw_data}")
            return

        df = pd.DataFrame(quotes)

        # ✅ 日付・銘柄コード
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df["ticker"] = code

        # ✅ カラム名の揺れを吸収
        COLUMN_MAP = {
            "O": "open",
            "H": "high",
            "L": "low",
            "Low": "low",
            "C": "price",
            "Vo": "volume",
        }
        df = df.rename(
            columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns}
        )

        required_cols = [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "price",
            "volume",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"必要なカラムが不足しています: {missing}")

        # ✅ 重複防止（最低限）
        df = df.drop_duplicates(subset=["ticker", "date"])

        # ✅ 保存
        db.save_prices(df[required_cols])
        print("✨ Supabaseへの保存が完了しました")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
        raise
