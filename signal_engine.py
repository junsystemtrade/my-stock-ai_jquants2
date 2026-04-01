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
import yaml
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
            "min_price":      200,
            "max_price":    50000,
            "min_data_days":   80,
        },
    }


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
    signals_cfg = cfg.get("signals", {})
    filter_cfg  = cfg.get("filter", {})
    results     = []

    min_days = filter_cfg.get("min_data_days", 80)
    if len(df) < min_days:
        return []

    close  = df["price"].astype(float)
    volume = df["volume"].astype(float)
    latest = close.iloc[-1]

    min_p = filter_cfg.get("min_price", 200)
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
            })

    for r in results:
        r["ticker"] = ticker
        r["price"]  = latest

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
# Gemini による企業調査
# -----------------------------------------------------------------------
def _research_company(
    client: genai.Client,
    ticker: str,
    company_name: str,
    signal_type: str,
    reason: str,
) -> dict:
    """
    Gemini にその企業の調査をさせる。
    売買シグナルや予想は出力させない。

    Returns
    -------
    dict:
        {
            "business":  str,  # 事業概要（何をしている会社か）
            "topic":     str,  # 直近トピック
            "context":   str,  # シグナルとの関連性（ファクトのみ）
        }
    失敗時は各フィールドにエラーメッセージを入れて返す。
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

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        raw = response.text.strip()

        # JSON パース
        import json, re
        # コードブロックが混入した場合も対応
        raw = re.sub(r"```json|```", "", raw).strip()
        data = json.loads(raw)

        return {
            "business": data.get("business", "（取得できませんでした）"),
            "topic":    data.get("topic",    "（取得できませんでした）"),
            "context":  data.get("context",  "（取得できませんでした）"),
        }

    except Exception as e:
        return {
            "business": "（企業調査エラー）",
            "topic":    f"({e})",
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

    # Gemini で企業調査
    print("🤖 Gemini で企業調査中...")
    results = []
    for sig in all_signals:
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
            "business":     research["business"],   # ← 事業概要（何をしている会社か）
            "topic":        research["topic"],       # ← 直近トピック
            "context":      research["context"],     # ← シグナルとの関連性
        })
        print(f"  ✅ {sig['ticker']} {company_name} ({sig['signal_type']}) 調査完了")

    return results
