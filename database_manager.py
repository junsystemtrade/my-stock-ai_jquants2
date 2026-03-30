import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")

        # ✅ psycopg2 + Supabase では余計な connect_args を一切指定しない
        self.engine = create_engine(db_url)

    def save_prices(self, df: pd.DataFrame):
        try:
            df.to_sql(
                "daily_prices",
                self.engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            print("✅ Supabaseへのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    def load_analysis_data(self, limit: int = 100) -> pd.DataFrame:
        query = f"""
            SELECT *
            FROM daily_prices
            ORDER BY date DESC
            LIMIT {limit}
        """
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                print(f"📖 DBから {len(df)} 件のデータをロードしました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
