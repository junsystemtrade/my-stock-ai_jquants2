import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            url = os.getenv("DATABASE_URL")
            if not url:
                raise ValueError("DATABASE_URL is missing in GitHub Secrets.")

            # プーラー(6543)経由での接続を安定させる設定
            DBManager._engine = create_engine(
                url,
                pool_pre_ping=True,      # 接続が切れていないか毎回確認
                pool_recycle=1800,       # 30分でコネクションをリフレッシュ
                connect_args={"connect_timeout": 30}
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        try:
            with self.engine.begin() as conn:
                df.to_sql("daily_prices", conn, if_exists="append", index=False)
            print(f"✅ {len(df)}件のデータを保存しました。")
        except Exception as e:
            print(f"❌ DB保存失敗: {e}")

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"⚠️ データ取得失敗（初回は正常）: {e}")
            return pd.DataFrame()
