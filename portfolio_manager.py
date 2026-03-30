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

    code = "30480"

    # ✅ 過去30営業日 ≒ 過去45暦日（土日祝を考慮して余裕を持つ）
    to_date = date.today() - timedelta(days=1)
    from_date = to_date - timedelta(days=45)

    params = {
        "code": code,
        "from": from_date.strftime("%Y%m%d"),
        "to": to_date.strftime("%Y%m%d"),
    }

    print(
        f"🎯 J-Quants V2 データ抽出開始: {code} "
        f"({params['from']} ～ {params['to']})"
    )

    res = requests.get(
        base_url,
        headers=headers,
        params=params,
        timeout=30,
    )
    res.raise_for_status()

    raw_data = res.json()
    quotes = raw_data.get("data", [])

    if not quotes:
        print("⚠️ データが取得できませんでした")
        return

    df = pd.DataFrame(quotes)

    # ✅ 日付・銘柄コード
    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = code

    # ✅ V2 カラム名変換
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

    # ✅ 日付昇順 & 重複除去
    df = (
        df[required_cols]
        .drop_duplicates(subset=["ticker", "date"])
        .sort_values("date")
    )

    db.save_prices(df)

    print(f"✨【完遂】{code} の {len(df)} 営業日分を Supabase に保存しました")


if __name__ == "__main__":
    sync_data()
``
