import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        raw_url = os.getenv("DATABASE_URL")
        if not raw_url:
            raise ValueError("DATABASE_URL is not set.")
        
        # Pooler接続エラー対策: ドットをコロンに変換
        # postgres.brhims... -> postgres:brhims...
        self.db_url = raw_url.replace("postgres.brhims", "postgres:brhims")
        self.engine = create_engine(self.db_url)

    def save_prices(self, df):
        if df is None or df.empty:
            return
        # テーブル名は適宜ご自身の環境に合わせてください
        df.to_sql("daily_prices", self.engine, if_exists="append", index=False)

    def load_analysis_data(self, days=150):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
