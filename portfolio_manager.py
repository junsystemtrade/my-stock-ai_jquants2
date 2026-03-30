import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
import database_manager

BASE_API = "https://api.jquants.com/v2"
EQ_DAILY_ENDPOINT = "/equities/bars/daily"


class RateLimiter:
    """
    Freeプランの 5 req/min を守るため、1リクエストあたり最低12秒以上あける。[2](https://jpx-jquants.com/en/spec/rate-limits)[3](https://jpx-jquants.com/spec/rate-limits)
    余裕を見てデフォルト 13秒。
    """
    def __init__(self, min_interval_sec: float = 13.0):
        self.min_interval = float(min_interval_sec)
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


def _headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Accept": "application/json"}


def _latest_accessible_date_from_code(api_key: str, limiter: RateLimiter, sample_code: str) -> date:
    """
    code指定で「その銘柄の取れる全期間」を取得し、末尾Dateを “Freeで取得可能な最新日” として採用する。
    /equities/bars/daily は code または date が必須で、code指定で特定銘柄のヒストリカルが取れる。[1](https://jpx-jquants.com/en/spec/eq-bars-daily)
    """
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    limiter.wait()
    r = requests.get(url, headers=_headers(api_key), params={"code": sample_code}, timeout=30)
    r.raise_for_status()

    data = r.json().get("data", [])
    if not data:
        raise RuntimeError("サンプル銘柄のデータが空でした（APIキー/銘柄コード/プランを確認）")

    # Date は YYYY-MM-DD で来る
    latest_str = data[-1]["Date"]
    return pd.to_datetime(latest_str).date()


def _fetch_all_issues_for_date(api_key: str, date_str: str, limiter: RateLimiter) -> list[dict]:
    """
    date指定で「その日の全銘柄」を取得。pagination_key で完走。[1](https://jpx-jquants.com/en/spec/eq-bars-daily)
    Freeはレートが厳しいので、ページめくりごとに limiter.wait() を必ず入れる。
    """
    url = f"{BASE_API}{EQ_DAILY_ENDPOINT}"
    rows: list[dict] = []
    pagination_key = None

    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        limiter.wait()
        r = requests.get(url, headers=_headers(api_key), params=params, timeout=30)

        # 429: レート制限。一定時間待って再試行するのが推奨。[2](https://jpx-jquants.com/en/spec/rate-limits)[3](https://jpx-jquants.com/spec/rate-limits)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "60"))
            time.sleep(retry_after)
            continue

        # 400: 非取引日/プラン範囲外などの可能性 → “その日は取れない” として空で返す
        if r.status_code == 400:
            return []

        r.raise_for_status()
        payload = r.json()

        rows.extend(payload.get("data", []))
        pagination_key = payload.get("pagination_key")

        if not pagination_key:
            break

    return rows


def _normalize_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    """
    APIレスポンス（Date, Code, O,H,L,C,Vo）をDB保存用（ticker, date, open, high, low, price, volume）へ整形。[1](https://jpx-jquants.com/en/spec/eq-bars-daily)
    """
    df = pd.DataFrame(rows)

    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df["ticker"] = df["Code"].astype(str)

    col_map = {"O": "open", "H": "high", "L": "low", "C": "price", "Vo": "volume"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    required = ["ticker", "date", "open", "high", "low", "price", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"必要カラム不足: {missing} / columns={list(df.columns)}")

    df = (
        df[required]
        .drop_duplicates(subset=["ticker", "date"])
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )
    return df


def sync_data():
    """
    Freeプラン向け:
      1) サンプル銘柄(code)の末尾Dateで “取得可能な最新日” を確定
      2) その日から過去へ、指定日数だけ “全銘柄(date指定)” を掘る
    """
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が設定されていません")

    db = database_manager.DBManager()

    # Freeの 5req/min を確実に守る [2](https://jpx-jquants.com/en/spec/rate-limits)[3](https://jpx-jquants.com/spec/rate-limits)
    limiter = RateLimiter(min_interval_sec=float(os.getenv("JQUANTS_MIN_INTERVAL_SEC", "13")))

    sample_code = os.getenv("SAMPLE_CODE_FOR_RANGE", "30480")
    days_per_run = int(os.getenv("BACKFILL_DAYS_PER_RUN", "1"))

    print("🚀 Free全銘柄バックフィル起動")
    print(f"   sample_code={sample_code}, days_per_run={days_per_run}, min_interval={limiter.min_interval}s")

    # ✅ “直近探索”はしない。codeから確定する
    latest_date = _latest_accessible_date_from_code(api_key, limiter, sample_code)
    print(f"✅ Freeで取得可能な最新日（code={sample_code}より）: {latest_date}")

    current_d = latest_date
    saved_total = 0

    for _ in range(days_per_run):
        # 土日をスキップ
        while current_d.weekday() >= 5:
            current_d -= timedelta(days=1)

        d_str = current_d.strftime("%Y-%m-%d")
        print(f"📥 全銘柄取得: {d_str}")

        rows = _fetch_all_issues_for_date(api_key, d_str, limiter)

        # 取れない日（400/空）はここで止める：= プラン上限/非取引日の可能性
        if not rows:
            print(f"⛔ {d_str}: 取得不可（非取引日/プラン範囲外の可能性）→ 今回は停止")
            break

        df = _normalize_rows_to_df(rows)
        db.save_prices(df)
        saved_total += len(df)

        print(f"✅ 保存完了: {d_str} / rows={len(df)}")

        # 前日へ
        current_d -= timedelta(days=1)

    print(f"🎉 今回の同期完了: saved_rows={saved_total}")


if __name__ == "__main__":
    sync_data()
