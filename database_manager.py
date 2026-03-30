import os
import pandas as pd
from sqlalchemy import create_engine

class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")
        # 接続の安定性を高める設定
        self.engine = create_engine(db_url)

    def save_prices(self, df: pd.DataFrame):
        try:
            # PostgreSQLへの一括保存
            df.to_sql(
                "daily_prices",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            print("✅ Supabaseへのデータ格納に成功しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    def load_analysis_data(self, days: int = 30) -> pd.DataFrame:
        """
        DB内の最新日を基準に、安全なバインド変数を用いて直近 N 日分を取得する
        """
        # ✅ 修正①: f-string を廃止し、%(days)s によるバインド変数化
        # ✅ 修正②: キャスト (::date) により型安全を確保
        query = """
            WITH latest AS (
                SELECT MAX(date::date) AS max_date
                FROM daily_prices
            )
            SELECT 
                p.ticker,
                p.date::date AS date,
                p.open,
                p.high,
                p.low,
                p.price,
                p.volume
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date::date >= (l.max_date - (INTERVAL '1 day' * %(days)s))
            ORDER BY p.date::date ASC
            LIMIT %(days)s
        """

        try:
            # ✅ params 引数で安全に値を渡す
            df = pd.read_sql(
                query,
                self.engine,
                params={"days": days},
            )

            if df.empty:
                print("⚠️ DBに該当期間のデータが存在しません。")
            else:
                print(f"📖 DBから最新日基準で {len(df)} 件のデータを安全にロードしました。")

            return df
        except Exception as e:
            print(f"⚠️ データ読み込みエラー: {e}")
            # エラー時は空のDataFrameを返し、後続のレポート処理を安全にスキップさせる
            return pd.DataFrame()
