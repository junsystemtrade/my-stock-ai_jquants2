import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        
        # ❗重要：URLにオプションを付けず、engine作成時に直接指定します
        self.engine = create_engine(
            db_url,
            connect_args={
                # これが psycopg2 に prepare_threshold を正しく認識させる書き方です
                "prepare_threshold": 0
            }
        )

    def save_prices(self, df):
        try:
            # 既にデータがあるかもしれないので append で追加
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✅ Supabase(6543)へのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")

    def load_analysis_data(self, days=30):
        # 最後に保存されたデータを確認
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                print(f"📖 DBから {len(df)} 件のデータをロードしました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
