import os
import pandas as pd
from sqlalchemy import create_engine, text

class DBManager:
    def __init__(self):
        # GitHub Secretsに登録したDATABASE_URLを環境変数から読み込みます
        # 形式: postgresql://postgres:[PASSWORD]@db.xxxx.supabase.co:5432/postgres
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url)

    def get_last_date(self, ticker):
        """特定の銘柄の最新保存日を確認（重複保存を防ぐため）"""
        query = text("SELECT MAX(date) FROM stock_prices WHERE ticker = :ticker")
        with self.engine.connect() as conn:
            result = conn.execute(query, {"ticker": ticker}).fetchone()
            return result[0] if result[0] else None

    def insert_prices(self, df, ticker):
        """取得した株価データを整形してSupabaseに保存"""
        if df.empty: return
        df = df.copy()
        
        # yfinanceのマルチインデックス（複数階層）構造を解消
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # 銘柄コード列を追加
        df['ticker'] = ticker
        # インデックス（日付）を列に変換し、小文字に統一（SQLとの整合性）
        df = df.reset_index().rename(columns={'Date': 'date', 'index': 'date'})
        df.columns = [c.lower() for c in df.columns]
        
        # 必要な列（銘柄、日付、始値、高値、安値、終値、出来高）のみを抽出
        df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        # DBへ書き込み（既存データがある場合は末尾に追加）
        df.to_sql("stock_prices", self.engine, if_exists="append", index=False, method='multi')

    def load_analysis_data(self, days=1825):
        """分析用に長期データをロード（デフォルトは5年分=1825日）"""
        print(f"📂 DBから過去 {days} 日分のデータを抽出中...")
        query = f"SELECT * FROM stock_prices WHERE date >= CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        df = pd.read_sql(query, self.engine)
        
        # 銘柄ごとにグループ化し、日付をインデックスにした辞書形式で返す
        return {t: group.set_index('date') for t, group in df.groupby('ticker')}
