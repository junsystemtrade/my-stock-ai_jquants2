"""
database_manager.py
===================
PostgreSQL（Supabase）との接続・データ操作を担当するモジュール。

変更点:
  - positions.entry_price を NULL 許容に変更
    （6:30 シグナル時は NULL で保存 → 翌日に始値を UPDATE）
  - update_entry_prices() を追加
    （entry_price IS NULL のポジションに前日の始値を設定）
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

    # ------------------------------------------------------------------
    # テーブル作成・マイグレーション
    # ------------------------------------------------------------------
    def _ensure_table(self):
        """テーブルの存在確認を高速に行い、必要な時だけ DDL を実行する"""
        check_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'daily_prices'
            );
        """)
        check_positions = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'positions'
            );
        """)

        try:
            with self.engine.connect() as conn:
                exists_prices    = conn.execute(check_query).scalar()
                exists_positions = conn.execute(check_positions).scalar()

            # daily_prices テーブル作成
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
                print("✅ daily_prices テーブルを新規作成しました")

            # positions テーブル作成
            if not exists_positions:
                # entry_price は NULL 許容（6:30 時点では始値が確定しないため）
                ddl_pos = """
                CREATE TABLE IF NOT EXISTS positions (
                    id           SERIAL PRIMARY KEY,
                    ticker       TEXT    NOT NULL,
                    entry_date   DATE    NOT NULL,
                    entry_price  NUMERIC,               -- NULL=未確定。翌日に始値で UPDATE
                    signal_type  TEXT,
                    status       TEXT    NOT NULL DEFAULT 'open',
                    closed_date  DATE,
                    close_reason TEXT,
                    exit_price   NUMERIC,
                    created_at   TIMESTAMP DEFAULT NOW(),
                    UNIQUE (ticker, entry_date)
                );
                CREATE INDEX IF NOT EXISTS idx_positions_status     ON positions (status);
                CREATE INDEX IF NOT EXISTS idx_positions_ticker     ON positions (ticker);
                CREATE INDEX IF NOT EXISTS idx_positions_entry_date ON positions (entry_date);
                """
                with self.engine.begin() as conn:
                    conn.execute(text("SET statement_timeout = '120s'"))
                    conn.execute(text(ddl_pos))
                print("✅ positions テーブルを新規作成しました")
            else:
                # 既存テーブルのマイグレーション
                with self.engine.begin() as conn:
                    # exit_price カラムがなければ追加
                    conn.execute(text("""
                        ALTER TABLE positions
                        ADD COLUMN IF NOT EXISTS exit_price NUMERIC;
                    """))
                    # entry_price の NOT NULL 制約を DROP（既存テーブル対応）
                    # PostgreSQL では ALTER COLUMN DROP NOT NULL で解除
                    conn.execute(text("""
                        ALTER TABLE positions
                        ALTER COLUMN entry_price DROP NOT NULL;
                    """))

        except Exception as e:
            print(f"⚠️ テーブル確認中にエラー（無視して続行）: {e}")

    # ------------------------------------------------------------------
    # 株価データ保存
    # ------------------------------------------------------------------
    def save_prices(self, df: pd.DataFrame):
        if df.empty:
            print("⚠️ 保存対象のデータが空です。スキップします。")
            return

        required = ["ticker", "date", "open", "high", "low", "price", "volume"]
        rows     = df[required].to_dict(orient="records")

        insert_sql = text("""
            INSERT INTO daily_prices (ticker, date, open, high, low, price, volume)
            VALUES (:ticker, :date, :open, :high, :low, :price, :volume)
            ON CONFLICT (ticker, date) DO NOTHING
        """)

        total_inserted = 0
        try:
            for i in range(0, len(rows), _DB_CHUNK_SIZE):
                chunk = rows[i: i + _DB_CHUNK_SIZE]
                with self.engine.begin() as conn:
                    conn.execute(text("SET statement_timeout = '60s'"))
                    conn.execute(insert_sql, chunk)
                total_inserted += len(chunk)
            print(f"✅ DB保存完了: {len(df):,} 件（チャンク分割実行完了）")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            raise

    # ------------------------------------------------------------------
    # ポジション保存（entry_price = NULL で保存）
    # ------------------------------------------------------------------
    def save_position(self, ticker: str, entry_date, entry_price, signal_type: str):
        """
        シグナル検知時（6:30）にポジションを保存する。
        entry_price は NULL を渡すこと（翌日に update_entry_prices() で始値を設定）。
        同日・同銘柄の重複は UNIQUE 制約で自動スキップ。
        """
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
                    "entry_price": entry_price,   # None を渡す
                    "signal_type": signal_type,
                })
            status = "NULL（翌日に始値を更新）" if entry_price is None else f"{entry_price}円"
            print(f"✅ ポジション保存: {ticker} / {entry_date} / entry_price={status}")
        except Exception as e:
            print(f"❌ ポジション保存エラー: {e}")

    # ------------------------------------------------------------------
    # 翌日の始値を entry_price に更新
    # ------------------------------------------------------------------
    def update_entry_prices(self) -> int:
        """
        entry_price が NULL のポジションに対して、
        daily_prices テーブルから entry_date 当日の始値（open）を設定する。

        【タイミング】
        翌日 6:30 のアクション実行時に呼ばれる。
        sync_data() でその日の株価が取得済みであることが前提。

        Returns: 更新した件数
        """
        update_sql = text("""
            UPDATE positions p
            SET entry_price = dp.open
            FROM daily_prices dp
            WHERE p.ticker       = dp.ticker
              AND p.entry_date   = dp.date
              AND p.entry_price  IS NULL
              AND p.status       = 'open'
              AND dp.open        IS NOT NULL
        """)
        try:
            with self.engine.begin() as conn:
                result = conn.execute(update_sql)
            return result.rowcount
        except Exception as e:
            print(f"❌ entry_price 更新エラー: {e}")
            return 0

    # ------------------------------------------------------------------
    # ポジション一覧取得
    # ------------------------------------------------------------------
    def load_open_positions(self) -> pd.DataFrame:
        """オープン中のポジションを全件取得する。"""
        sql = text("""
            SELECT id, ticker, entry_date, entry_price, signal_type,
                   status, closed_date, close_reason, exit_price
            FROM positions
            WHERE status = 'open'
            ORDER BY entry_date DESC
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            print(f"📂 オープンポジション: {len(df)} 件")
            return df
        except Exception as e:
            print(f"❌ ポジション読み込みエラー: {e}")
            return pd.DataFrame()

    def close_position(
        self, ticker: str, entry_date,
        close_reason: str, closed_date, exit_price: float
    ):
        """ポジションをクローズする。"""
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
            SELECT ticker, entry_date, entry_price,
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
            SELECT ticker, entry_date, entry_price,
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

    # ------------------------------------------------------------------
    # 株価データ取得
    # ------------------------------------------------------------------
    def get_latest_saved_date(self) -> str | None:
        query = text("SELECT MAX(date) AS max_date FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        return str(result[0]) if result and result[0] else None

    def get_oldest_saved_date(self) -> str | None:
        query = text("SELECT MIN(date) AS min_date FROM daily_prices")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        return str(result[0]) if result and result[0] else None

    def load_analysis_data(self, days: int = 150) -> pd.DataFrame:
        """最新日基準で直近 N 日以内の全銘柄データをロード。"""
        query = text("""
            WITH latest AS (
                SELECT MAX(date) AS max_date FROM daily_prices
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
                return pd.read_sql(query, conn, params={"ticker": ticker})
        except Exception as e:
            print(f"❌ 銘柄データ読み込みエラー ({ticker}): {e}")
            return pd.DataFrame()
