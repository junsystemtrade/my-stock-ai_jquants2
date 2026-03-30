import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # GitHub SecretsからURLを取得
        db_url = os.getenv("DATABASE_URL")
        
        # ❗URLに直接オプションを付けず、engine側で「準備済みステートメント」を無効化
        self.engine = create_engine(
            db_url,
            connect_args={
                "prepare_threshold": 0, # これでPgBouncer(6543)のエラーを回避
                "options": "-c statement_timeout=30000"
            }
        )

    def save_prices(self, df):
        """データをSupabaseへ保存"""
        try:
            # table名 'daily_prices' に保存
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✅ Supabaseへのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")

    # ❗【重要】backtest_engineが探し回っていたメソッドを追加
    def load_analysis_data(self, days=30):
        """分析用にデータをDBから読み出す"""
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        try:
            df = pd.read_sql(query, self.engine)
            print(f"📖 DBから {len(df)} 件のデータを読み込みました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
