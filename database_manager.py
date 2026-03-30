import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None
    def __init__(self):
        if DBManager._engine is None:
            # URLから ?prepare_threshold=0 などのオプションを消した純粋なURLを読み込む
            raw_url = os.getenv("DATABASE_URL")
            # もしURLにオプションが含まれていたら強制的にカット
            url = raw_url.split('?')[0]
            
            # 接続時にオプションを渡す
            DBManager._engine = create_engine(
                url,
                pool_pre_ping=True,
                connect_args={
                    "prepare_threshold": 0,
                    "connect_timeout": 30
                }
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        with self.engine.begin() as conn:
            df.to_sql("daily_prices", conn, if_exists="append", index=False)

    def load_analysis_data(self, days=30):
        # データが空でもエラーにならないよう安全に取得
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        try:
            return pd.read_sql(query, self.engine)
        except:
            return pd.DataFrame()
