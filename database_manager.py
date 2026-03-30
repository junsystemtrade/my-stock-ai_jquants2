import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta


class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")

        # psycopg2 + Supabase 用の安定構成
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
        直近 N 日分のデータを取得する（営業日ベース）
        """
        start_date = date.today() - timedelta(days=days)

        query = """
            SELECT *
            FROM daily_prices
            WHERE date >= %(start_date)s
            ORDER BY date ASC
        """

        try:
            df = pd.read_sql(
                query,
                self.engine,
                params={"start_date": start_date},
            )

            if not df.empty:
                print(f"📖 DBから {len(df)} 件のデータをロードしました。")
            else:
                print("⚠️ DBに該当期間のデータが存在しません。")

            return df

        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
