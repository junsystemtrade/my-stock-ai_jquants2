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
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    code = "30480"

    # ✅ from / to / date を一切指定しない
    params = {
        "code": code
    }

    print(f"🎯 J-Quants V2 データ抽出開始: {code}（取得可能な全期間）")

    res = requests.get(
        base_url,
        headers=headers,
        params=params,
        timeout=30,
    )
    res.raise_for_status()

    quotes = res.json().get("data", [])
    if not quotes:
        print("⚠️ データが取得できませんでした")
        return

    df = pd.DataFrame(quotes)

    # ✅ 整形
    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = code

    COLUMN_MAP = {
        "O": "open",
        "H": "high",
        "L": "low",
        "Low": "low",
        "C": "price",
        "Vo": "volume",
    }
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

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

    # ✅ 日付昇順・重複排除
    df = (
        df[required_cols]
        .drop_duplicates(subset=["ticker", "date"])
        .sort_values("date")
    )

    db.save_prices(df)

    print(f"✨【完遂】{code} の {len(df)} 件を Supabase に保存しました")


if __name__ == "__main__":
    sync_data()
