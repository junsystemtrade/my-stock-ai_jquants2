import os
import pandas as pd
from sqlalchemy import create_engine, text

class DBManager:
    def __init__(self):
        # GitHub SecretsのDATABASE_URLを使用
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url)

    def get_last_date(self, ticker):
        """銘柄ごとの最終更新日を取得"""
        query = text("SELECT MAX(date) FROM stock_prices WHERE ticker = :ticker")
        with self.engine.connect() as conn:
            result = conn.execute(query, {"ticker": ticker}).fetchone()
            return result[0] if result[0] else None

    def insert_prices(self, df, ticker):
        """株価データを整形して保存"""
        if df.empty: return
        df = df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df['ticker'] = ticker
        df = df.reset_index().rename(columns={'Date': 'date', 'index': 'date'})
        df.columns = [c.lower() for c in df.columns]
        df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        df.to_sql("stock_prices", self.engine, if_exists="append", index=False, method='multi')

    def load_analysis_data(self, days=1825):
        """5年分(1825日)のデータを一括ロード"""
        query = f"SELECT * FROM stock_prices WHERE date >= CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        df = pd.read_sql(query, self.engine)
        return {t: group.set_index('date') for t, group in df.groupby('ticker')}
