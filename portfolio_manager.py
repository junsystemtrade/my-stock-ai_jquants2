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

    # J-Quants V2 デイリーバー（日足）エンドポイント
    base_url = "https://api.jquants.com/v2/equities/bars/daily"
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    # ✅ 銘柄コード: 30480 (ビックカメラ)
    code = "30480"

    # 🚀 修正ポイント: 過去30日分の範囲を指定して一括取得します
    # これにより、DBに十分なデータが貯まり、Geminiの分析が走るようになります。
    today_str = date.today().strftime("%Y%m%d")
    from_str = (date.today() - timedelta(days=30)).strftime("%Y%m%d")

    params = {
        "code": code,
        "from": from_str,
        "to": today_str
    }

    print(f"🎯 J-Quants V2 データ抽出開始: {code} ({from_str} ～ {today_str})")

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
            print(f"⚠️ 取得されたデータが空でした。レスポンス: {raw_data}")
            return

        df = pd.DataFrame(quotes)

        # ✅ 日付と銘柄コードの整形
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df["ticker"] = code

        # ✅ カラム名の揺れを吸収
        COLUMN_MAP = {
            "O": "open", "H": "high", "L": "low", "Low": "low", "C": "price", "Vo": "volume"
        }
        df = df.rename(
            columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns}
        )

        # 保存に必要なカラム
        required_cols = ["ticker", "date", "open", "high", "low", "price", "volume"]
        
        # 重複を除去（(ticker, date) がユニークになるように）
        df = df.drop_duplicates(subset=["ticker", "date"])

        # ✅ 保存（method='multi' で高速化）
        db.save_prices(df[required_cols])
        print(f"✨ {code} の過去30日分のデータを Supabase へ同期完了しました！")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
        raise

if __name__ == "__main__":
    sync_data()
