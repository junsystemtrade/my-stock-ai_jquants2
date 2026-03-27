import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            raw_url = os.getenv("DATABASE_URL")
            # postgres.プロジェクトID の形式に強制補完
            project_id = "brhimsggmuhvothbkmbm"
            if f"postgres.{project_id}" not in raw_url:
                url = raw_url.replace("postgres:", f"postgres.{project_id}:")
            else:
                url = raw_url
                
            DBManager._engine = create_engine(
                url,
                pool_pre_ping=True,
                connect_args={
                    "connect_timeout": 30,
                    "options": "-c prepare_threshold=0"
                }
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        with self.engine.begin() as conn:
            df.to_sql("daily_prices", conn, if_exists="append", index=False)

    def load_analysis_data(self, days=30):
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        return pd.read_sql(query, self.engine)
