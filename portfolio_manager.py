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

    # ✅ 銘柄コード（5桁）
    code = "30480"

    print(f"🎯 J-Quants V2 データ抽出開始: {code}（過去30営業日分）")

    all_rows = []
    collected_days = 0
    max_days = 30

    # 今日から遡る（十分余裕を持って 60 日分見る）
    d = date.today() - timedelta(days=1)

    while collected_days < max_days:
        # 土日はスキップ
        if d.weekday() >= 5:
            d -= timedelta(days=1)
            continue

        date_str = d.strftime("%Y%m%d")

        params = {
            "code": code,
            "date": date_str,
        }

        try:
            res = requests.get(
                base_url,
                headers=headers,
                params=params,
                timeout=20,
            )

            # ❗ 非営業日・祝日は 400 が返る → 静かにスキップ
            if res.status_code == 400:
                d -= timedelta(days=1)
                continue

            res.raise_for_status()
            raw_data = res.json()
            quotes = raw_data.get("data", [])

            if quotes:
                all_rows.extend(quotes)
                collected_days += 1
                print(f"  ✅ 取得成功: {date_str}（{collected_days}/{max_days}）")

        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ スキップ: {date_str} ({e})")

        d -= timedelta(days=1)

    if not all_rows:
        print("❌ 有効な株価データを1件も取得できませんでした")
        return

    # ✅ DataFrame 化
    df = pd.DataFrame(all_rows)

    # ✅ 日付・銘柄コード
    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = code

    # ✅ カラム名マッピング（V2対応）
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

    # ✅ 重複排除（保険）
    df = df.drop_duplicates(subset=["ticker", "date"])

    # ✅ DBへ保存
    db.save_prices(df[required_cols])

    print(f"✨【完遂】{code} の過去 {len(df)} 営業日分を Supabase に保存しました")


if __name__ == "__main__":
    sync_data()
