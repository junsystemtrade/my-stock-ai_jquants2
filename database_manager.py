import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            url = os.getenv("DATABASE_URL")
            # ユーザー名が 'postgres' だけの場合、プロジェクトIDを自動補完
            project_id = "brhimsggmuhvothbkmbm"
            if f"postgres.{project_id}" not in url:
                url = url.replace("postgres:", f"postgres.{project_id}:")
            
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
        print(f"✅ DB保存完了: {len(df)}件")

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        try:
            return pd.read_sql(query, self.engine)
        except Exception:
            return pd.DataFrame()
