import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            url = os.getenv("DATABASE_URL")
            if not url:
                raise ValueError("DATABASE_URL is not set.")

            # IPv4接続を安定させるための工夫
            # 1. pool_sizeを小さくして、Supabaseの制限(15)に合わせる
            # 2. max_overflowを0にして、予期せぬ増殖を防ぐ
            DBManager._engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=5, 
                max_overflow=0,
                pool_recycle=300,
                connect_args={"connect_timeout": 10}
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        # 接続を明示的に閉じるようにコンテキストマネージャを使用
        with self.engine.begin() as conn:
            df.to_sql("daily_prices", conn, if_exists="append", index=False)

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
