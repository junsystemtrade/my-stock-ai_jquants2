import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")
        self.engine = create_engine(db_url)

    def save_prices(self, df: pd.DataFrame):
        try:
            df.to_sql("daily_prices", self.engine, if_exists="append", index=False, method="multi")
            print("✅ Supabaseへのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    def load_analysis_data(self, days: int = 30) -> pd.DataFrame:
        """
        最新日を基準にバインド変数 %(days)s を使って安全に取得
        """
        query = """
            WITH latest AS (
                SELECT MAX(date::date) AS max_date
                FROM daily_prices
            )
            SELECT 
                p.ticker, p.date::date AS date, p.open, p.high, p.low, p.price, p.volume
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date::date >= (l.max_date - (INTERVAL '1 day' * %(days)s))
            ORDER BY p.date::date ASC
            LIMIT %(days)s
        """
        try:
            df = pd.read_sql(query, self.engine, params={"days": days})
            if not df.empty:
                print(f"📖 DBから最新日基準で {len(df)} 件を安全にロードしました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
