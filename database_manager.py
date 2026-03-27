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
                raise ValueError("DATABASE_URL is missing.")

            # Supabase Nanoプラン(接続数制限)に最適化
            DBManager._engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=0,
                # ネットワーク不安定対策のタイムアウト設定
                connect_args={"connect_timeout": 30}
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        # トランザクションを明示して確実にクローズ
        with self.engine.begin() as conn:
            df.to_sql("daily_prices", conn, if_exists="append", index=False)

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
