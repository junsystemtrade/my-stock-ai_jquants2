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
        check_positions_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'positions'
            );
        """)

        try:
            with self.engine.connect() as conn:
                exists = conn.execute(check_query).scalar()
                exists_positions = conn.execute(check_positions_query).scalar()

            if not exists:
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
                    conn.execute(text("SET statement_timeout = '120s'"))
                    conn.execute(text(ddl))
                    print("✅ daily_pricesテーブルを新規作成しました")

            if not exists_positions:
                ddl_positions = """
                CREATE TABLE IF NOT EXISTS positions (
                    ticker       TEXT    NOT NULL,
                    entry_date   DATE    NOT NULL,
                    entry_price  NUMERIC NOT NULL,
                    signal_type  TEXT,
                    status       TEXT    NOT NULL DEFAULT 'open',
                    closed_date  DATE,
                    close_reason TEXT,
                    exit_price   NUMERIC,
                    PRIMARY KEY (ticker, entry_date)
                );
                CREATE INDEX IF NOT EXISTS idx_positions_status ON positions (status);
                """
                with self.engine.begin() as conn:
                    conn.execute(text("SET statement_timeout = '120s'"))
                    conn.execute(text(ddl_positions))
                    print("✅ positionsテーブルを新規作成しました")
            else:
                # 既存テーブルに exit_price カラムがなければ追加（マイグレーション）
                with self.engine.begin() as conn:
                    conn.execute(text("""
                        ALTER TABLE positions
                        ADD COLUMN IF NOT EXISTS exit_price NUMERIC;
                    """))

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
            for i in range(0, len(rows), _DB_CHUNK_SIZE):
                chunk = rows[i : i + _DB_CHUNK_SIZE]
                with self.engine.begin() as conn:
                    conn.execute(text("SET statement_timeout = '60s'"))
                    conn.execute(insert_sql, chunk)
                total_inserted += len(chunk)
            
            print(f"✅ DB保存完了: {len(df):,} 件（チャンク分割実行完了）")
            
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    def save_position(self, ticker: str, entry_date, entry_price: float, signal_type: str):
        """買いシグナル発生時にポジションを保存する。"""
        sql = text("""
            INSERT INTO positions (ticker, entry_date, entry_price, signal_type, status)
            VALUES (:ticker, :entry_date, :entry_price, :signal_type, 'open')
            ON CONFLICT (ticker, entry_date) DO NOTHING
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(sql, {
                    "ticker": ticker,
                    "entry_date": entry_date,
                    "entry_price": entry_price,
                    "signal_type": signal_type,
                })
            print(f"✅ ポジション保存: {ticker} / {entry_date}")
        except Exception as e:
            print(f"❌ ポジション保存エラー: {e}")

    def load_open_positions(self) -> pd.DataFrame:
        """オープン中のポジションを全件取得する。"""
        sql = text("""
            SELECT ticker, entry_date, entry_price, signal_type
            FROM positions
            WHERE status = 'open'
            ORDER BY entry_date ASC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            print(f"📂 オープンポジション: {len(df)} 件")
            return df
        except Exception as e:
            print(f"❌ ポジション読み込みエラー: {e}")
            return pd.DataFrame()

    def close_position(self, ticker: str, entry_date, close_reason: str, closed_date, exit_price: float):
        """ポジションをクローズする。exit_price を記録する。"""
        sql = text("""
            UPDATE positions
            SET status       = 'closed',
                close_reason = :close_reason,
                closed_date  = :closed_date,
                exit_price   = :exit_price
            WHERE ticker     = :ticker
              AND entry_date = :entry_date
              AND status     = 'open'
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(sql, {
                    "ticker":       ticker,
                    "entry_date":   entry_date,
                    "close_reason": close_reason,
                    "closed_date":  closed_date,
                    "exit_price":   exit_price,
                })
            print(f"✅ ポジションクローズ: {ticker} / {close_reason} / 売値:{exit_price}円")
        except Exception as e:
            print(f"❌ ポジションクローズエラー: {e}")

    def load_weekly_trades(self) -> pd.DataFrame:
        """直近7日間にクローズしたトレードを取得する。"""
        sql = text("""
            SELECT
                ticker, entry_date, entry_price,
                closed_date, exit_price, close_reason, signal_type
            FROM positions
            WHERE status = 'closed'
              AND closed_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY closed_date ASC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            print(f"📊 今週のクローズトレード: {len(df)} 件")
            return df
        except Exception as e:
            print(f"❌ 週次トレード読み込みエラー: {e}")
            return pd.DataFrame()

    def load_all_closed_trades(self) -> pd.DataFrame:
        """累計の全クローズトレードを取得する。"""
        sql = text("""
            SELECT
                ticker, entry_date, entry_price,
                closed_date, exit_price, close_reason, signal_type
            FROM positions
            WHERE status = 'closed'
              AND exit_price IS NOT NULL
            ORDER BY closed_date ASC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            print(f"📊 累計クローズトレード: {len(df)} 件")
            return df
        except Exception as e:
            print(f"❌ 累計トレード読み込みエラー: {e}")
            return pd.DataFrame()

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
