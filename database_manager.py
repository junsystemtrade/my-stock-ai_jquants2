import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # GitHub Secrets の DATABASE_URL を使用
        self.db_url = os.getenv("DATABASE_URL")
        
        if not self.db_url:
            print("❌ エラー: DATABASE_URL が環境変数に設定されていません。")
            raise ValueError("DATABASE_URL is missing.")

        # デバッグ用：接続先ホストの一部を表示（パスワードは隠す）
        host_info = self.db_url.split('@')[-1] if '@' in self.db_url else "Unknown"
        print(f"🔗 データベース接続試行中... (Target: {host_info})")
        
        # 直接接続 (5432) を想定したエンジン作成
        self.engine = create_engine(self.db_url)

    def save_prices(self, df):
        if df is None or df.empty:
            return
        try:
            # 既存データがある場合は追記
            df.to_sql("daily_prices", self.engine, if_exists="append", index=False)
            print(f"✅ DB保存完了: {len(df)} 件")
        except Exception as e:
            print(f"❌ DB保存失敗: {e}")

    def load_analysis_data(self, days=30):
        query = f"SELECT * FROM daily_prices WHERE date > CURRENT_DATE - INTERVAL '{days} days' ORDER BY date ASC"
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"❌ データ読み込み失敗: {e}")
            return pd.DataFrame()
