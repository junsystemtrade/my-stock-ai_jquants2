import os
import pandas as pd
from sqlalchemy import create_engine
import socket

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        
        # ❗GitHub Actionsのネットワーク問題を回避する設定
        # IPv6で迷子になるのを防ぎ、IPv4で確実に接続させます
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5
            }
        )

    def save_prices(self, df):
        try:
            # 既存のテーブルにデータを流し込む
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✅ Supabase(5432)へのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")

    def load_analysis_data(self, days=30):
        # Gemini分析用にデータをロード
        query = "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 50"
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                print(f"📖 DBから {len(df)} 件のデータをロードしました。")
            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
