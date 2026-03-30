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
        DBにある最新の日付から、指定された行数（営業日分）を取得する
        """
        # ❗修正ポイント: 日付計算ではなく、最新から N 件を直接取得します
        query = f"""
            SELECT *
            FROM daily_prices
            ORDER BY date DESC
            LIMIT {days}
        """

        try:
            df = pd.read_sql(query, self.engine)

            if not df.empty:
                # Geminiが時系列順に読めるよう、昇順に並べ替えてから返します
                df = df.sort_values("date").reset_index(drop=True)
                print(f"📖 DBから最新 {len(df)} 件をロード。最新日: {df['date'].max()}")
            else:
                print("⚠️ DBにデータが1件も存在しません。")

            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            return pd.DataFrame()
