import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL が設定されていません。")

        self.engine = create_engine(db_url)

    def insert_price_data(self, df: pd.DataFrame):
        df.to_sql("prices", self.engine, if_exists="append", index=False)

    def load_analysis_data(self):
        query = "SELECT * FROM prices ORDER BY date DESC LIMIT 300;"
        return pd.read_sql(query, self.engine)
