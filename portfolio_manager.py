import os
import requests
import pandas as pd
from datetime import date, timedelta
import database_manager


def get_latest_trading_day(api_key: str) -> str:
    """
    J-Quants に date を指定せず問い合わせ、
    実在する最新営業日の日付を取得する
    """
    base_url = "https://api.jquants.com/v2/equities/bars/daily"
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    res = requests.get(
        base_url,
        headers=headers,
        params={"code": "30480"},
        timeout=20,
    )
    res.raise_for_status()

    data = res.json().get("data", [])
    if not data:
        raise RuntimeError("最新営業日の取得に失敗しました")

    return data[0]["Date"]  # YYYY-MM-DD


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

    # ✅ 実在する最新営業日を API から取得
    latest_date_str = get_latest_trading_day(api_key)
    latest_date = pd.to_datetime(latest_date_str).date()

    # ✅ 過去30営業日 ≒ 45暦日
    from_date = latest_date - timedelta(days=45)

    params = {
        "code": code,
        "from": from_date.strftime("%Y%m%d"),
        "to": latest_date.strftime("%Y%m%d"),
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

    quotes = res.json().get("data", [])
    if not quotes:
        print("⚠️ データが取得できませんでした")
        return

    df = pd.DataFrame(quotes)

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

    df = (
        df[required_cols]
        .drop_duplicates(subset=["ticker", "date"])
        .sort_values("date")
    )

    db.save_prices(df)

    print(f"✨【完遂】{code} の {len(df)} 営業日分を Supabase に保存しました")


if __name__ == "__main__":
    sync_data()
