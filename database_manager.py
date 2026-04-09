"""
database_manager.py
===================
PostgreSQL（Supabase）との接続・データ操作を担当するモジュール。
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# 一度に INSERT する行数。タイムアウト対策のため環境変数で調整可能にする
_DB_CHUNK_SIZE = int(os.getenv("DB_CHUNK_SIZE", "1000"))


class DBManager:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL が設定されていません")
        
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                "connect_timeout": 30,
                # 基本はタイムアウトなしにするが、DDL実行時などは個別に設定
                "options": "-c statement_timeout=0"
            },
        )
        self._ensure_table()

    def _ensure_table(self):
        """テーブルの存在確認を高速に行い、必要な時だけ DDL を実行する"""
        check_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'daily_prices'
            );
        """)
        
        try:
            with self.engine.connect() as conn:
                exists = conn.execute(check_query).scalar()
            
            if exists:
                return

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
                # テーブル作成時は長めのタイムアウトを許容
                conn.execute(text("SET statement_timeout = '120s'"))
                conn.execute(text(ddl))
                print("✅ データベーステーブルを新規作成しました")

        except Exception as e:
            print(f"⚠️ テーブル確認中にエラー（無視して続行）: {e}")

    def save_prices(self, df: pd.DataFrame):
        """
        タイムアウト対策を施した保存処理。
        _DB_CHUNK_SIZE ごとにトランザクションを確定（コミット）させる。
        """
        if df.empty:
            print("⚠️ 保存対象のデータが空です。スキップします。")
            return

        required = ["ticker", "date", "open", "high", "low", "price", "volume"]
        rows = df[required].to_dict(orient="records")

        insert_sql = text("""
            INSERT INTO daily_prices (ticker, date, open, high, low, price, volume)
            VALUES (:ticker, :date, :open, :high, :low, :price, :volume)
            ON CONFLICT (ticker, date) DO NOTHING
        """)

        total_inserted = 0
        try:
            # 大量データを一気に INSERT するとインデックス更新でタイムアウトするため分割実行
            for i in range(0, len(rows), _DB_CHUNK_SIZE):
                chunk = rows[i : i + _DB_CHUNK_SIZE]
                # self.engine.begin() をループ内で使うことで、各チャンクごとに COMMIT される
                with self.engine.begin() as conn:
                    # 個別のチャンクに対してもステートメントタイムアウトを設定（安全策）
                    conn.execute(text("SET statement_timeout = '60s'"))
                    conn.execute(insert_sql, chunk)
                total_inserted += len(chunk)
            
            print(f"✅ DB保存完了: {len(df):,} 件（チャンク分割実行完了）")
            
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    def get_latest_saved_date(self) -> str | None:
        """保存済みの最新日を返す。"""
        query = text("SELECT MAX(date) AS max_date FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        if result and result[0]:
            return str(result[0])
        return None

    def get_oldest_saved_date(self) -> str | None:
        """保存済みの最古日を返す（バックフィルの再開用）。"""
        query = text("SELECT MIN(date) AS min_date FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        if result and result[0]:
            return str(result[0])
        return None

    def load_analysis_data(self, days: int = 150) -> pd.DataFrame:
        """最新日基準で直近 N 日以内の全銘柄データをロード。"""
        query = text("""
            WITH latest AS (
                SELECT MAX(date) AS max_date
                FROM daily_prices
            )
            SELECT
                p.ticker, p.date, p.open, p.high, p.low, p.price, p.volume
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date >= (l.max_date - (:days * INTERVAL '1 day'))
            ORDER BY p.ticker, p.date ASC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"days": days})
            if df.empty:
                print("⚠️ DBにデータがありません。")
            else:
                tickers = df["ticker"].nunique()
                print(f"📖 DBロード完了: {tickers:,} 銘柄 × {len(df):,} 件（直近 {days} 日以内）")
            return df
        except Exception as e:
            print(f"❌ データ読み込みエラー: {e}")
            return pd.DataFrame()

    def load_ticker_data(self, ticker: str) -> pd.DataFrame:
        """特定銘柄の全期間データを返す（バックテスト用）。"""
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
