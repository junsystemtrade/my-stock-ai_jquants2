import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        url = os.getenv("DATABASE_URL")
        # IPv4接続を安定させるためのオプションを追加
        # GitHub Actions環境での 'Network is unreachable' 対策
        self.engine = create_engine(
            url,
            connect_args={"connect_timeout": 10}
        )

    def save_prices(self, df):
        if df is None or df.empty: return
        df.to_sql("daily_prices", self.engine, if_exists="append", index=False)

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
