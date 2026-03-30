import os
import pandas as pd
from sqlalchemy import create_engine


class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")
        self.engine = create_engine(db_url)

    def save_prices(self, df: pd.DataFrame):
        df.to_sql(
            "daily_prices",
            self.engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        print("✅ Supabaseへのデータ格納に成功しました！")

    def load_analysis_data(self, days: int = 30) -> pd.DataFrame:
        """
        DB に存在する最新日を基準に、直近 N 日分を取得する
        ※ date カラムが TEXT でも必ず動くように CAST する
        """
        query = """
            WITH latest AS (
                SELECT MAX(date::date) AS max_date
                FROM daily_prices
            )
            SELECT
                p.ticker,
                p.date::date AS date,
                p.open,
                p.high,
                p.low,
                p.price,
                p.volume
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date::date >= (l.max_date - (INTERVAL '1 day' * %(days)s))
            ORDER BY p.date::date ASC
        """

        df = pd.read_sql(
            query,
            self.engine,
            params={"days": days},
        )

        if df.empty:
            print("⚠️ DBに該当期間のデータが存在しません。")
        else:
            print(f"📖 DBから最新日基準で {len(df)} 件のデータをロードしました。")

        return df
