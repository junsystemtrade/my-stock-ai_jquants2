import os
import pandas as pd
from sqlalchemy import create_engine, text

class DBManager:
    def __init__(self):
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL is not set.")
        
        # Supabase Pooler (6543) + SSL 必須 + pre_ping で安定化
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True
        )

    def get_last_date(self, ticker):
        query = text("SELECT MAX(date) FROM stock_prices WHERE ticker = :ticker")
        with self.engine.connect() as conn:
            return conn.execute(query, {"ticker": ticker}).scalar()

    def insert_prices(self, df, ticker):
        if df.empty:
            return
        
        temp_df = df.reset_index()
        temp_df.columns = [c.lower() for c in temp_df.columns]
        temp_df["ticker"] = ticker

        target_cols = ["date", "open", "high", "low", "close", "volume", "ticker"]
        temp_df[target_cols].to_sql(
            "stock_prices",
            self.engine,
            if_exists="append",
            index=False
        )

    def load_analysis_data(self, days=150):
        query = f"""
        SELECT * FROM stock_prices
        WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY ticker, date ASC
        """
        return pd.read_sql(query, self.engine)
