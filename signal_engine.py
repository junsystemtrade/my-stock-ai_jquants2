"""
signal_engine.py
================
テクニカルシグナルの計算と、シグナルを満たした銘柄の AI 企業調査を担当。

設計方針:
  - シグナル判定はコードで行う（AI は予想しない）
  - Gemini は「その企業がどんな会社か」の調査のみ行う
  - シグナル条件は signals_config.yml で管理
  - Gemini SDK: google-genai（新・公式）使用

公開インターフェース:
  scan_signals(daily_data: pd.DataFrame) -> list[dict]
"""

import os
import time
import yaml
import json
import re
import pandas as pd
from pathlib import Path
from google import genai


# -----------------------------------------------------------------------
# シグナル設定の読み込み
# -----------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent / "signals_config.yml"

def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {
        "signals": {
            "golden_cross": {"enabled": True, "short_window": 25, "long_window": 75},
            "rsi_oversold":  {"enabled": True, "window": 14, "threshold": 30},
            "volume_surge":  {"enabled": True, "window": 20, "multiplier": 2.0},
        },
        "filter": {
            "min_price":            500,
            "max_price":          50000,
            "min_data_days":         80,
            "exclude_code_range": [[1000, 1999]],
            "max_signals_per_ticker": 1,
        },
    }


# -----------------------------------------------------------------------
# ETF・REIT 除外チェック
# -----------------------------------------------------------------------
def _is_excluded(ticker: str, cfg: dict) -> bool:
    """
    ETF・REIT・インフラファンドなどを除外する。
    signals_config.yml の exclude_code_range で設定した範囲を除外。
    例: [1000, 1999] → 1000.T〜1999.T を除外
    """
    filter_cfg = cfg.get("filter", {})
    exclude_ranges = filter_cfg.get("exclude_code_range", [])

    try:
        code = int(ticker.replace(".T", "").strip())
    except ValueError:
        return False

    for r in exclude_ranges:
        if len(r) == 2 and r[0] <= code <= r[1]:
            return True
    return False


# -----------------------------------------------------------------------
# テクニカル指標の計算
# -----------------------------------------------------------------------
def _calc_sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def _calc_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(window=window, min_periods=window).mean()
    loss  = (-delta.clip(upper=0)).rolling(window=window, min_periods=window).mean()
    rs    = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


# -----------------------------------------------------------------------
# シグナル判定（1 銘柄ぶん）
# -----------------------------------------------------------------------
def _check_signals(ticker: str, df: pd.DataFrame, cfg: dict) -> list[dict]:
    """
    1 銘柄の DataFrame に対してシグナル判定を行う。
    max_signals_per_ticker の設定により、最も強いシグナル1件のみ返す。
    """
    signals_cfg = cfg.get("signals", {})
    filter_cfg  = cfg.get("filter", {})
    results     = []

    # ETF・REIT 除外
    if _is_excluded(ticker, cfg):
        return []

    min_days = filter_cfg.get("min_data_days", 80)
    if len(df) < min_days:
        return []

    close  = df["price"].astype(float)
    volume = df["volume"].astype(float)
    latest = close.iloc[-1]

    # 価格フィルター
    min_p = filter_cfg.get("min_price", 500)
    max_p = filter_cfg.get("max_price", 50000)
    if not (min_p <= latest <= max_p):
        return []

    # ---- ① ゴールデンクロス ----
    gc_cfg = signals_cfg.get("golden_cross", {})
    if gc_cfg.get("enabled", True):
        short_w = gc_cfg.get("short_window", 25)
        long_w  = gc_cfg.get("long_window", 75)
        sma_s   = _calc_sma(close, short_w)
        sma_l   = _calc_sma(close, long_w)
        if (
            len(sma_s.dropna()) >= 2
            and len(sma_l.dropna()) >= 2
            and sma_s.iloc[-2] <= sma_l.iloc[-2]
            and sma_s.iloc[-1]  > sma_l.iloc[-1]
        ):
            results.append({
                "signal_type": "ゴールデンクロス",
                "reason": f"短期MA({short_w}日){sma_s.iloc[-1]:.0f}円が長期MA({long_w}日){sma_l.iloc[-1]:.0f}円を上抜け",
                "priority": 1,
            })

    # ---- ② RSI 売られすぎ ----
    rsi_cfg   = signals_cfg.get("rsi_oversold", {})
    if rsi_cfg.get("enabled", True):
        rsi_w     = rsi_cfg.get("window", 14)
        threshold = rsi_cfg.get("threshold", 30)
        rsi       = _calc_rsi(close, rsi_w)
        if not rsi.isna().iloc[-1] and rsi.iloc[-1] < threshold:
            results.append({
                "signal_type": "RSI売られすぎ",
                "reason": f"RSI({rsi_w}日): {rsi.iloc[-1]:.1f}（閾値 {threshold} を下回る）",
                "priority": 2,
            })

    # ---- ③ 出来高急増 ----
    vol_cfg = signals_cfg.get("volume_surge", {})
    if vol_cfg.get("enabled", True):
        vol_w = vol_cfg.get("window", 20)
        mult  = vol_cfg.get("multiplier", 2.0)
        avg_v = volume.iloc[-(vol_w + 1):-1].mean()
        if avg_v > 0 and volume.iloc[-1] >= avg_v * mult:
            results.append({
                "signal_type": "出来高急増",
                "reason": f"当日出来高 {volume.iloc[-1]:,.0f} が{vol_w}日平均の {mult}倍超（平均: {avg_v:,.0f}）",
                "priority": 3,
            })

    if not results:
        return []

    # 1銘柄あたりの最大シグナル数（デフォルト1件）
    max_signals = filter_cfg.get("max_signals_per_ticker", 1)
    results     = sorted(results, key=lambda x: x["priority"])[:max_signals]

    for r in results:
        r["ticker"] = ticker
        r["price"]  = latest
        r.pop("priority", None)

    return results


# -----------------------------------------------------------------------
# 会社名の取得
# -----------------------------------------------------------------------
def _get_company_name(ticker: str, ticker_map: dict) -> str:
    info = ticker_map.get(ticker, {})
    name = info.get("name", "")
    if name and name not in ("nan", "None", ""):
        return name
    return ticker.replace(".T", "")


# -----------------------------------------------------------------------
# Gemini による企業調査（リトライ付き）
# -----------------------------------------------------------------------
def _research_company(
    client: genai.Client,
    ticker: str,
    company_name: str,
    signal_type: str,
    reason: str,
    max_retries: int = 3,
) -> dict:
    """
    Gemini にその企業の調査をさせる。
    429（クォータ超過）の場合はリトライする。
    """
    code = ticker.replace(".T", "")

    prompt = f"""あなたは企業リサーチアナリストです。
以下の日本株について調査し、指定のJSON形式で回答してください。

銘柄コード: {code}
会社名: {company_name}
検知シグナル: {signal_type}（{reason}）

【出力形式】必ず以下のJSON形式のみで回答すること。説明文・前置き・コードブロック記号は不要。
{{
  "business": "主力事業・業種を1〜2文で説明（例: 家電量販店を全国展開。PC・スマホ・白物家電が主力）",
  "topic": "直近の注目IR・ニュース・動向を1文で（なければ「特になし」）",
  "context": "今回のシグナルと企業状況の関連性をファクトベースで1文（予想・推奨は禁止）"
}}"""

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-1.5-flash",   # 無料枠が大きいモデルを使用
                contents=prompt,
            )
            raw = response.text.strip()
            raw = re.sub(r"```json|```", "", raw).strip()
            data = json.loads(raw)

            return {
                "business": data.get("business", "（取得できませんでした）"),
                "topic":    data.get("topic",    "（取得できませんでした）"),
                "context":  data.get("context",  "（取得できませんでした）"),
            }

        except Exception as e:
            err_str = str(e)
            # 429クォータ超過 → 待機してリトライ
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                wait_sec = 30 * (attempt + 1)  # 30秒、60秒、90秒と増やす
                print(f"  ⚠️ Gemini 429 クォータ超過 → {wait_sec}秒待機してリトライ ({attempt+1}/{max_retries})")
                time.sleep(wait_sec)
                continue
            # その他のエラー
            return {
                "business": "（企業調査エラー）",
                "topic":    f"({e})",
                "context":  "",
            }

    return {
        "business": "（Gemini クォータ超過。時間をおいて再実行してください）",
        "topic":    "特になし",
        "context":  "",
    }


# -----------------------------------------------------------------------
# 公開インターフェース
# -----------------------------------------------------------------------
def scan_signals(daily_data: pd.DataFrame) -> list[dict]:
    """
    全銘柄の株価データからテクニカルシグナルをスキャンし、
    条件を満たした銘柄について Gemini で企業調査を行う。

    Returns
    -------
    list[dict]
        {ticker, company_name, price, signal_type, reason,
         business, topic, context} のリスト
    """
    if daily_data.empty:
        print("⚠️ データが空です。シグナルスキャンをスキップします。")
        return []

    cfg = _load_config()

    # Gemini クライアント初期化
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY が設定されていません")
    client = genai.Client(api_key=api_key)

    # 会社名マスタ取得
    try:
        from portfolio_manager import get_target_tickers
        ticker_map = get_target_tickers()
    except Exception:
        ticker_map = {}

    # 銘柄ごとにシグナル判定
    all_signals: list[dict] = []
    tickers = daily_data["ticker"].unique()
    print(f"🔍 シグナルスキャン開始: {len(tickers):,} 銘柄")

    for ticker in tickers:
        df_ticker = (
            daily_data[daily_data["ticker"] == ticker]
            .sort_values("date")
            .reset_index(drop=True)
        )
        hits = _check_signals(ticker, df_ticker, cfg)
        all_signals.extend(hits)

    print(f"📊 シグナル検知: {len(all_signals)} 件")

    if not all_signals:
        return []

    # Gemini で企業調査（シグナル間に1秒待機してクォータを節約）
    print("🤖 Gemini で企業調査中...")
    results = []
    for i, sig in enumerate(all_signals):
        company_name = _get_company_name(sig["ticker"], ticker_map)
        research     = _research_company(
            client,
            sig["ticker"],
            company_name,
            sig["signal_type"],
            sig["reason"],
        )
        results.append({
            "ticker":       sig["ticker"],
            "company_name": company_name,
            "price":        sig["price"],
            "signal_type":  sig["signal_type"],
            "reason":       sig["reason"],
            "business":     research["business"],
            "topic":        research["topic"],
            "context":      research["context"],
        })
        print(f"  ✅ {sig['ticker']} {company_name} ({sig['signal_type']}) 調査完了")

        # シグナル間に少し待機してクォータを節約
        if i < len(all_signals) - 1:
            time.sleep(2)

    return results
