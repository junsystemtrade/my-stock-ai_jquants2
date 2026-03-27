import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    _engine = None

    def __init__(self):
        if DBManager._engine is None:
            # Secretsから完全なURLを読み込む
            url = os.getenv("DATABASE_URL")
            if not url:
                raise ValueError("DATABASE_URL が設定されていません。")

            # プーラー接続用の最適化設定
            DBManager._engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_recycle=1800,
                connect_args={
                    "connect_timeout": 30
                }
            )
        self.engine = DBManager._engine

    def save_prices(self, df):
        if df is None or df.empty: return
        try:
            with self.engine.begin() as conn:
                # 重複を避ける場合は本来処理が必要ですが、まずは単純保存
                df.to_sql("daily_prices", conn, if_exists="append", index=False)
            print(f"✅ DB保存成功: {len(df)}件")
        except Exception as e:
            print(f"❌ DB保存失敗: {e}")

    def load_analysis_data(self, days=30):
        # backtest_engine が呼び出すメソッド
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 100"
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"⚠️ データ取得失敗: {e}")
            return pd.DataFrame()
