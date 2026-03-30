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

    # ❗ポイント: 
    # 特定の日付を指定すると、土日祝日の場合に400エラーになります。
    # params から "date" を外すことで、APIが保持する「直近の有効な営業日データ」を自動取得します。
    params = {
        "code": code
    }

    print(f"🎯 J-Quants V2 データ抽出開始: {code} (最新営業日をリクエスト)")

    try:
        res = requests.get(
            base_url,
            headers=headers,
            params=params,
            timeout=20,
        )
        # 400や500エラーが出た場合に例外を発生させる
        res.raise_for_status()

        raw_data = res.json()
        # V2のデータ構造は {"data": [...]} です
        quotes = raw_data.get("data", [])

        if not quotes:
            print(f"⚠️ 取得されたデータが空でした。レスポンス: {raw_data}")
            return

        df = pd.DataFrame(quotes)

        # ✅ 日付と銘柄コードの整形
        # J-Quantsの 'Date' カラムをPythonの日付型に変換
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df["ticker"] = code

        # ✅ V2のカラム名揺れ（Low / L など）をマッピングで吸収
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

        # 保存に必要なカラムが揃っているか最終確認
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

        # 万が一の重複を除去
        df = df.drop_duplicates(subset=["ticker", "date"])

        # ✅ DBManager経由でSupabaseへ保存
        db.save_prices(df[required_cols])
        print(f"✨ {code} のデータを Supabase へ同期完了しました！")

    except requests.exceptions.HTTPError as e:
        print(f"❌ APIリクエスト失敗: {e}")
        raise
    except Exception as e:
        print(f"❌ 実行エラー: {e}")
        raise

if __name__ == "__main__":
    sync_data()
