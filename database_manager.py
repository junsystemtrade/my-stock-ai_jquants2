import os
import pandas as pd
from sqlalchemy import create_engine, text

class DBManager:
    def __init__(self):
        # GitHub Secrets から DATABASE_URL を取得
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL is not set.")
        
        # SQLAlchemyエンジン作成 (Supabase Pooler経由の安定接続)
        self.engine = create_engine(db_url, pool_pre_ping=True)

    def get_last_date(self, ticker):
        """銘柄ごとの最終更新日を取得"""
        query = text("SELECT MAX(date) FROM stock_prices WHERE ticker = :ticker")
        with self.engine.connect() as conn:
            result = conn.execute(query, {"ticker": ticker}).scalar()
            return result

    def insert_prices(self, df, ticker):
        """Yahoo FinanceのデータをDBに保存"""
        if df.empty:
            return
        
        # DataFrameをDB形式に整形
        temp_df = df.reset_index()
        temp_df.columns = [c.lower() for c in temp_df.columns]
        temp_df['ticker'] = ticker
        
        # 必要なカラムのみ抽出して保存
        target_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
        temp_df[target_cols].to_sql('stock_prices', self.engine, if_exists='append', index=False)

    def load_analysis_data(self, days=150):
        """分析用に過去データをロード"""
        query = f"""
        SELECT * FROM stock_prices 
        WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY ticker, date ASC
        """
        return pd.read_sql(query, self.engine)
