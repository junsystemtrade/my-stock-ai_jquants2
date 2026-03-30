import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import date

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")
        # psycopg2 + Supabase 用の標準構成
        self.engine = create_engine(db_url)

    def save_prices(self, df: pd.DataFrame):
        try:
            df.to_sql(
                "daily_prices",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            print("✅ Supabaseへのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    def load_analysis_data(self, days: int = 30) -> pd.DataFrame:
        """
        DB に存在する最新日を基準に、直近 N 日分を取得する
        """
        # ❗ PostgreSQL では INTERVAL '1 day' * :days の形式が最も安全です
        query = """
            WITH latest AS (
                SELECT MAX(date) AS max_date
                FROM daily_prices
            )
            SELECT p.*
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date >= (l.max_date - (INTERVAL '1 day' * %(days)s))
            ORDER BY p.date ASC
        """

        try:
            df = pd.read_sql(
                query,
                self.engine,
                params={"days": days},
            )

            if df.empty:
                print("⚠️ DBに該当期間のデータが存在しません。")
            else:
                # 重複排除が必要な場合はここで行う
                df = df.drop_duplicates(subset=["ticker", "date"])
                print(f"📖 DBから最新日基準で {len(df)} 件のデータをロードしました。")

            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
