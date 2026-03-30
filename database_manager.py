import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        
        # ❗修正ポイント：URLから余計なクエリパラメータを完全に除去し、
        # 接続オプションを別で渡すことで psycopg2 のエラーを回避します。
        if "?" in db_url:
            db_url = db_url.split("?")[0]
            
        self.engine = create_engine(
            db_url,
            connect_args={
                # ここに書くのではなく、execution_options で制御するのが最も安全です
                "options": "-c statement_timeout=30000"
            },
            # ❗これがPgBouncer(6543ポート)でのエラーを消す決定打です
            execution_options={
                "prepared_statement_name_func": lambda name: None
            }
        )

    def save_prices(self, df):
        try:
            # 既存のテーブルに追記
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✨【DB着弾】Supabaseへの格納に成功しました！")
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
