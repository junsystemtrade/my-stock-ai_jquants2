import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # GitHub Secretsから取得
        raw_url = os.getenv("DATABASE_URL")
        # 接続文字列がドット形式ならコロン形式に補正（Pooler対策）
        self.db_url = raw_url.replace("postgres.brhims", "postgres:brhims")
        self.engine = create_engine(self.db_url)

    def save_prices(self, df):
        if df.empty: return
        df.to_sql("daily_prices", self.engine, if_exists="append", index=False)

    def load_analysis_data(self, days=150):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
