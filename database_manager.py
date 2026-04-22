"""
database_manager.py
===================
PostgreSQL（Supabase）との接続・データ操作を担当するモジュール。

変更点:
  - save_prices: ON CONFLICT DO NOTHING → DO UPDATE（open=NULLの行を上書き）
  - load_analysis_data: days パラメータを CAST(INTEGER) で明示キャスト
  - _ensure_table: 毎回実行されていたALTERを migrate() に切り出し
    （初回セットアップ時のみ手動実行する）
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

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
                "options":         "-c statement_timeout=0",
            },
        )
        self._ensure_table()

    def _ensure_table(self):
        """テーブルが存在しない場合のみ作成する。ALTERは含まない。"""
        check_prices    = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'daily_prices');")
        check_positions = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'positions');")

        try:
            with self.engine.connect() as conn:
                exists_prices    = conn.execute(check_prices).scalar()
                exists_positions = conn.execute(check_positions).scalar()

            if not exists_prices:
                ddl = """
                CREATE TABLE IF NOT EXISTS daily_prices (
                    ticker  TEXT    NOT NULL,
                    date    DATE    NOT NULL,
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
                print("OK daily_prices table created")

            if not exists_positions:
                ddl_pos = """
                CREATE TABLE IF NOT EXISTS positions (
                    ticker       TEXT    NOT NULL,
                    entry_date   DATE    NOT NULL,
                    entry_price  NUMERIC,
                    signal_type  TEXT,
                    status       TEXT    NOT NULL DEFAULT 'open',
                    closed_date  DATE,
                    close_reason TEXT,
                    exit_price   NUMERIC,
                    PRIMARY KEY (ticker, entry_date)
                );
                CREATE INDEX IF NOT EXISTS idx_positions_status     ON positions (status);
                CREATE INDEX IF NOT EXISTS idx_positions_ticker     ON positions (ticker);
                CREATE INDEX IF NOT EXISTS idx_positions_entry_date ON positions (entry_date);
                """
                with self.engine.begin() as conn:
                    conn.execute(text("SET statement_timeout = '120s'"))
                    conn.execute(text(ddl_pos))
                print("OK positions table created")

        except Exception as e:
            print(f"WARNING table check error (continuing): {e}")

    def migrate(self):
        """
        初回セットアップ時のみ手動で呼び出すマイグレーション。
        既存テーブルへのカラム追加・制約変更を行う。

        実行方法:
            from database_manager import DBManager
            DBManager().migrate()
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text("ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_price NUMERIC;"))
                conn.execute(text("ALTER TABLE positions ALTER COLUMN entry_price DROP NOT NULL;"))
            print("OK migration completed")
        except Exception as e:
            print(f"ERROR migration: {e}")

    # ------------------------------------------------------------------
    # 株価データ保存
    # ------------------------------------------------------------------
    def save_prices(self, df: pd.DataFrame):
        """
        株価データをDBに保存する。
        同じ (ticker, date) が既に存在する場合、open が NULL であれば上書きする。
        open が既に設定済みの行は更新しない（不要なI/Oを避ける）。
        """
        if df.empty:
            print("WARNING: empty dataframe, skip save")
            return

        required   = ["ticker", "date", "open", "high", "low", "price", "volume"]
        rows       = df[required].to_dict(orient="records")
        insert_sql = text("""
            INSERT INTO daily_prices (ticker, date, open, high, low, price, volume)
            VALUES (:ticker, :date, :open, :high, :low, :price, :volume)
            ON CONFLICT (ticker, date) DO UPDATE
                SET open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    price  = EXCLUDED.price,
                    volume = EXCLUDED.volume
                WHERE daily_prices.open IS NULL
        """)
        total = 0
        try:
            for i in range(0, len(rows), _DB_CHUNK_SIZE):
                chunk = rows[i: i + _DB_CHUNK_SIZE]
                with self.engine.begin() as conn:
                    conn.execute(text("SET statement_timeout = '60s'"))
                    conn.execute(insert_sql, chunk)
                total += len(chunk)
            print(f"OK DB save: {len(df):,} rows")
        except Exception as e:
            print(f"ERROR DB save: {e}")
            raise

    # ------------------------------------------------------------------
    # ポジション保存（entry_price = NULL で保存）
    # ------------------------------------------------------------------
    def save_position(self, ticker: str, entry_date, entry_price, signal_type: str):
        sql = text("""
            INSERT INTO positions (ticker, entry_date, entry_price, signal_type, status)
            VALUES (:ticker, :entry_date, :entry_price, :signal_type, 'open')
            ON CONFLICT (ticker, entry_date) DO NOTHING
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(sql, {
                    "ticker":      ticker,
                    "entry_date":  entry_date,
                    "entry_price": entry_price,
                    "signal_type": signal_type,
                })
            status = "NULL(翌日更新)" if entry_price is None else f"{entry_price}円"
            print(f"OK position saved: {ticker} / {entry_date} / entry_price={status}")
        except Exception as e:
            print(f"ERROR position save: {e}")

    # ------------------------------------------------------------------
    # 翌日の始値を entry_price に更新
    # ------------------------------------------------------------------
    def update_entry_prices(self) -> int:
        """entry_price が NULL のポジションに当日の始値を設定する"""
        update_sql = text("""
            UPDATE positions p
            SET entry_price = dp.open
            FROM daily_prices dp
            WHERE p.ticker      = dp.ticker
              AND p.entry_date  = dp.date
              AND p.entry_price IS NULL
              AND p.status      = 'open'
              AND dp.open       IS NOT NULL
        """)
        try:
            with self.engine.begin() as conn:
                result = conn.execute(update_sql)
            return result.rowcount
        except Exception as e:
            print(f"ERROR update_entry_prices: {e}")
            return 0

    # ------------------------------------------------------------------
    # ポジション一覧取得
    # ------------------------------------------------------------------
    def load_open_positions(self) -> pd.DataFrame:
        sql = text("""
            SELECT ticker, entry_date, entry_price, signal_type,
                   status, closed_date, close_reason, exit_price
            FROM positions
            WHERE status = 'open'
            ORDER BY entry_date DESC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            print(f"OK open positions: {len(df)}")
            return df
        except Exception as e:
            print(f"ERROR load_open_positions: {e}")
            return pd.DataFrame()

    def close_position(self, ticker: str, entry_date, close_reason: str, closed_date, exit_price: float):
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
            print(f"OK position closed: {ticker} / {close_reason} / {exit_price}yen")
        except Exception as e:
            print(f"ERROR close_position: {e}")

    def load_weekly_trades(self) -> pd.DataFrame:
        sql = text("""
            SELECT ticker, entry_date, entry_price,
                   closed_date, exit_price, close_reason, signal_type
            FROM positions
            WHERE status = 'closed'
              AND closed_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY closed_date ASC
        """)
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(sql, conn)
        except Exception as e:
            print(f"ERROR load_weekly_trades: {e}")
            return pd.DataFrame()

    def load_all_closed_trades(self) -> pd.DataFrame:
        sql = text("""
            SELECT ticker, entry_date, entry_price,
                   closed_date, exit_price, close_reason, signal_type
            FROM positions
            WHERE status = 'closed'
              AND exit_price IS NOT NULL
            ORDER BY closed_date ASC
        """)
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(sql, conn)
        except Exception as e:
            print(f"ERROR load_all_closed_trades: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 株価データ取得
    # ------------------------------------------------------------------
    def get_latest_saved_date(self) -> str | None:
        query = text("SELECT MAX(date) FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        return str(result[0]) if result and result[0] else None

    def get_oldest_saved_date(self) -> str | None:
        query = text("SELECT MIN(date) FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        return str(result[0]) if result and result[0] else None

    def load_analysis_data(self, days: int = 150) -> pd.DataFrame:
        query = text("""
            WITH latest AS (SELECT MAX(date) AS max_date FROM daily_prices)
            SELECT p.ticker, p.date, p.open, p.high, p.low, p.price, p.volume
            FROM daily_prices p
            CROSS JOIN latest l
            WHERE p.date >= (l.max_date - (CAST(:days AS INTEGER) * INTERVAL '1 day'))
            ORDER BY p.ticker, p.date ASC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"days": days})
            if not df.empty:
                print(f"OK DB load: {df['ticker'].nunique():,} tickers x {len(df):,} rows (last {days} days)")
            return df
        except Exception as e:
            print(f"ERROR load_analysis_data: {e}")
            return pd.DataFrame()

    def load_ticker_data(self, ticker: str) -> pd.DataFrame:
        query = text("""
            SELECT ticker, date, open, high, low, price, volume
            FROM daily_prices WHERE ticker = :ticker ORDER BY date ASC
        """)
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn, params={"ticker": ticker})
        except Exception as e:
            print(f"ERROR load_ticker_data ({ticker}): {e}")
            return pd.DataFrame()
