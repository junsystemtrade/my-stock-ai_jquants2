import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # GitHub Secrets の修正後のURLを取得
        db_url = os.getenv("DATABASE_URL")
        
        # 接続の安定性を高めるための最小限の設定
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True, # 接続切れを自動検知
            connect_args={
                "prepare_threshold": 0 # PgBouncer対策
            }
        )

    def save_prices(self, df):
        try:
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✅ Supabaseへのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")

    def load_analysis_data(self, days=30):
        # 最新のデータを読み出す
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 50"
        try:
            df = pd.read_sql(query, self.engine)
            print(f"📖 DBから {len(df)} 件のデータをロードしました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
