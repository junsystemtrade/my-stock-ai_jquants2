import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # GitHub Secretsから取得
        raw_url = os.getenv("DATABASE_URL")
        if not raw_url:
            raise ValueError("DATABASE_URL is not set in environment variables.")
        
        # 接続文字列がドット形式ならコロン形式に補正（Supabase Pooler対策）
        # postgres.project_id -> postgres:project_id
        self.db_url = raw_url.replace("postgres.brhims", "postgres:brhims")
        
        # SQLAlchemyエンジン作成
        self.engine = create_engine(self.db_url)

    def save_prices(self, df):
        if df is None or df.empty:
            return
        # 重複を避けるため、既存のデータに追記（必要に応じて調整）
        df.to_sql("daily_prices", self.engine, if_exists="append", index=False)

    def load_analysis_data(self, days=150):
        query = f"""
        SELECT * FROM daily_prices 
        WHERE date > CURRENT_DATE - INTERVAL '{days} days' 
        ORDER BY date ASC
        """
        return pd.read_sql(query, self.engine)
