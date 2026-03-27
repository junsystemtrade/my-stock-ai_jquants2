import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise ValueError("DATABASE_URL is missing.")
        # ポート5432の直接接続を推奨
        self.engine = create_engine(self.db_url)

    def save_prices(self, df):
        if df is None or df.empty: return
        df.to_sql("daily_prices", self.engine, if_exists="append", index=False)

    def load_analysis_data(self, days=150):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
