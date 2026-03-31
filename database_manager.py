import os
import pandas as pd
from sqlalchemy import create_engine, text


class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")
        self.engine = create_engine(db_url)
        self._ensure_table()

    # ------------------------------------------------------------------
    # テーブル初期化（初回起動時に自動作成）
    # ------------------------------------------------------------------
    def _ensure_table(self):
        """
        daily_prices テーブルが存在しない場合のみ作成する。
        (ticker, date) を PRIMARY KEY にすることで重複を DB レベルで防ぐ。
        """
        ddl = """
        CREATE TABLE IF NOT EXISTS daily_prices (
            ticker  TEXT        NOT NULL,
            date    DATE        NOT NULL,
            open    NUMERIC,
            high    NUMERIC,
            low     NUMERIC,
            price   NUMERIC,
            volume  BIGINT,
            PRIMARY KEY (ticker, date)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_prices_date   ON daily_prices (date);
        CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices (ticker);
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

    # ------------------------------------------------------------------
    # データ保存（差分 upsert）
    # ------------------------------------------------------------------
    def save_prices(self, df: pd.DataFrame):
        """
        (ticker, date) が重複する場合は何もしない (ON CONFLICT DO NOTHING)。
        初回大量投入も毎日差分投入もこれ一本で対応できる。
        """
        if df.empty:
            print("⚠️ 保存対象のデータが空です。スキップします。")
            return

        rows = df[["ticker", "date", "open", "high", "low", "price", "volume"]].to_dict(orient="records")

        insert_sql = text("""
            INSERT INTO daily_prices (ticker, date, open, high, low, price, volume)
            VALUES (:ticker, :date, :open, :high, :low, :price, :volume)
            ON CONFLICT (ticker, date) DO NOTHING
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(insert_sql, rows)
            print(f"✅ DB保存完了: {len(rows)} 件（重複はスキップ）")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    # ------------------------------------------------------------------
    # DBに保存済みの最終日を取得（差分取得に使う）
    # ------------------------------------------------------------------
    def get_latest_saved_date(self) -> str | None:
        """
        daily_prices に保存されている最新の date を返す。
        1件もない場合は None を返す。
        """
        query = text("SELECT MAX(date) AS max_date FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        if result and result[0]:
            return str(result[0])  # "YYYY-MM-DD" 形式
        return None

    # ------------------------------------------------------------------
    # 分析用データのロード
    # ------------------------------------------------------------------
    def load_analysis_data(self, days: int = 150) -> pd.DataFrame:
        """
        最新日を基準に直近 N 営業日分の全銘柄データをロードする。

        ポイント:
          - LIMIT を使わず「日付の範囲」で絞ることで、
            全銘柄 × N 日分を正しく取得できる。
          - days=150 なら 約150営業日 × 全銘柄 が対象になる。
        """
        query = text("""
            WITH latest AS (
                SELECT MAX(date) AS max_date
                FROM daily_prices
            )
            SELECT
                p.ticker,
                p.date,
                p.open,
                p.high,
                p.low,
                p.price,
                p.volume
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date >= (l.max_date - (:days * INTERVAL '1 day'))
            ORDER BY p.ticker, p.date ASC
        """)

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"days": days})
            if df.empty:
                print("⚠️ DBにデータがありません。先にデータ取得を実行してください。")
            else:
                tickers = df["ticker"].nunique()
                print(f"📖 DBロード完了: {tickers} 銘柄 × {len(df)} 件（直近 {days} 日以内）")
            return df
        except Exception as e:
            print(f"❌ データ読み込みエラー: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 特定銘柄の全期間データをロード（バックテスト用）
    # ------------------------------------------------------------------
    def load_ticker_data(self, ticker: str) -> pd.DataFrame:
        """
        特定の銘柄の全期間データを返す。バックテストで使う。
        """
        query = text("""
            SELECT ticker, date, open, high, low, price, volume
            FROM daily_prices
            WHERE ticker = :ticker
            ORDER BY date ASC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"ticker": ticker})
            return df
        except Exception as e:
            print(f"❌ 銘柄データ読み込みエラー ({ticker}): {e}")
            return pd.DataFrame()
