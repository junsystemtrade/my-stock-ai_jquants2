import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        # 5432ポートの新しいURLを読み込み
        db_url = os.getenv("DATABASE_URL")
        
        # 直通ポートなので、標準的なエンジン作成でOK
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True
        )

    def save_prices(self, df):
        """J-Quantsから取得したデータを保存"""
        try:
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✅ Supabase(5432)への格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")

    def load_analysis_data(self, days=30):
        """Gemini分析用にデータをロード"""
        # 最新の50件を取得
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 50"
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                print(f"📖 DBから {len(df)} 件のデータをロードしました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
