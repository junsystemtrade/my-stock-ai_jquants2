import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # GitHub Secrets の DATABASE_URL (Direct: 5432)
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)

    def save_prices(self, df):
        if df is None or df.empty: return
        
        # カラムを整理
        cols = ['ticker', 'date', 'open', 'high', 'low', 'price', 'volume']
        df_save = df[cols].copy()
        
        try:
            # 既存データがある場合は一旦追記
            # 理想はDB側で UNIQUE (ticker, date) 制約を貼り、
            # ON CONFLICT DO UPDATE を使うことですが、まずは簡易的に append
            df_save.to_sql("daily_prices", self.engine, if_exists="append", index=False)
        except Exception:
            # 重複エラーが出た場合は1件ずつ、または無視
            pass

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        return pd.read_sql(query, self.engine)
