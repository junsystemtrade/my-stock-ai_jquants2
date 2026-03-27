import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            url = os.getenv("DATABASE_URL")
            # prepare_threshold は URL ではなく、ここで安全に処理
            DBManager._engine = create_engine(
                url,
                pool_pre_ping=True,
                connect_args={
                    "connect_timeout": 30,
                    # プーラー利用時のエラーを避けるための設定
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
        except Exception as e:
            print(f"⚠️ データ取得失敗（初回は正常）: {e}")
            return pd.DataFrame()
