import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None
    def __init__(self):
        if DBManager._engine is None:
            url = os.getenv("DATABASE_URL")
            DBManager._engine = create_engine(
                url, 
                connect_args={"options": "-c prepare_threshold=0"}
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        with self.engine.begin() as conn:
            df.to_sql("daily_prices", conn, if_exists="append", index=False)

    def load_analysis_data(self, days=30):
        """分析用に最新データを取得するメソッド（追加）"""
        query = f"SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
