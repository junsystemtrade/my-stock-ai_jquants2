import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            url = os.getenv("DATABASE_URL")
            # 接続文字列から余計なオプションを排除し、ここで設定
            DBManager._engine = create_engine(
                url,
                pool_pre_ping=True,
                connect_args={
                    "connect_timeout": 30,
                    # プーラー利用時に必要な設定をここで渡す
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
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        try:
            return pd.read_sql(query, self.engine)
        except Exception:
            return pd.DataFrame()
