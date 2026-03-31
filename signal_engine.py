"""
signal_engine.py
================
テクニカルシグナルの計算と、シグナルを満たした銘柄の AI 企業調査を担当。

設計方針:
  - シグナル判定はコードで行う（AI は予想しない）
  - Gemini は「その企業がどんな会社か」の調査のみ行う
  - シグナル条件は signals_config.yml で管理（コード変更なしでチューニング可能）

公開インターフェース:
  scan_signals(daily_data: pd.DataFrame) -> list[dict]
    → main.py から呼ばれる。シグナルが立った銘柄のリストを返す。
    → 各要素: {ticker, price, signal_type, reason, insight}
"""

import os
import yaml
import pandas as pd
import google.generativeai as genai
from pathlib import Path


# -----------------------------------------------------------------------
# シグナル設定の読み込み
# -----------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent / "signals_config.yml"

def _load_config() -> dict:
    """signals_config.yml を読み込む。ファイルがなければデフォルト値を使う。"""
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f)
    # デフォルト設定（yml がない場合のフォールバック）
    return {
        "signals": {
            "golden_cross": {
                "enabled": True,
                "short_window": 25,
                "long_window": 75,
            },
            "rsi_oversold": {
                "enabled": True,
                "window": 14,
                "threshold": 30,
            },
            "volume_surge": {
                "enabled": True,
                "window": 20,
                "multiplier": 2.0,
            },
        },
        "filter": {
            "min_price": 200,
            "max_price": 50000,
            "min_data_days": 80,   # 計算に必要な最低データ数
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
    """
    1 銘柄の DataFrame に対してシグナル判定を行い、
    条件を満たしたシグナルのリストを返す。
    """
    signals_cfg = cfg.get("signals", {})
    filter_cfg  = cfg.get("filter", {})
    results     = []

    # データ不足チェック
    min_days = filter_cfg.get("min_data_days", 80)
    if len(df) < min_days:
        return []

    close   = df["price"].astype(float)
    volume  = df["volume"].astype(float)
    latest  = close.iloc[-1]

    # 価格フィルター
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
            and sma_s.iloc[-2] <= sma_l.iloc[-2]   # 前日: short ≤ long
            and sma_s.iloc[-1]  > sma_l.iloc[-1]   # 当日: short > long
        ):
            results.append({
                "signal_type": "ゴールデンクロス",
                "reason": (
                    f"短期MA({short_w}日){sma_s.iloc[-1]:.0f}円が"
                    f"長期MA({long_w}日){sma_l.iloc[-1]:.0f}円を上抜け"
                ),
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
                "reason": (
                    f"RSI({rsi_w}日): {rsi.iloc[-1]:.1f}"
                    f"（閾値 {threshold} を下回る）"
                ),
            })

    # ---- ③ 出来高急増 ----
    vol_cfg = signals_cfg.get("volume_surge", {})
    if vol_cfg.get("enabled", True):
        vol_w  = vol_cfg.get("window", 20)
        mult   = vol_cfg.get("multiplier", 2.0)
        avg_v  = volume.iloc[-(vol_w + 1):-1].mean()  # 直近 N 日の平均（当日除く）

        if avg_v > 0 and volume.iloc[-1] >= avg_v * mult:
            results.append({
                "signal_type": "出来高急増",
                "reason": (
                    f"当日出来高 {volume.iloc[-1]:,.0f} が"
                    f"{vol_w}日平均の {mult}倍超（平均: {avg_v:,.0f}）"
                ),
            })

    # シグナルが 1 つ以上あれば ticker と price を付与して返す
    for r in results:
        r["ticker"] = ticker
        r["price"]  = latest

    return results


# -----------------------------------------------------------------------
# Gemini による企業調査（売買予想は行わない）
# -----------------------------------------------------------------------
def _research_company(client, ticker: str, signal_type: str, reason: str) -> str:
    """
    Gemini にその企業がどんな会社かを調査させる。
    売買シグナルや予想は出力させない。
    """
    # ticker 例: "30480.T" → コード "3048"
    code = ticker.replace(".T", "")

    prompt = f"""
あなたは企業リサーチアナリストです。
以下の銘柄について、企業の基本情報を調査・整理してください。

銘柄コード: {code}
検知されたシグナル: {signal_type}
シグナル詳細: {reason}

【出力内容】（売買の予想・推奨は一切しないこと）
1. 企業名と事業概要（主力事業、業種）
2. 直近の注目トピック（IR、新製品、提携など）
3. 競合・業界ポジション
4. このシグナルと企業状況の関連性（ファクトのみ、予想禁止）

200文字以内で簡潔にまとめてください。
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        return response.text.strip()
    except Exception as e:
        return f"（企業調査エラー: {e}）"


# -----------------------------------------------------------------------
# 公開インターフェース
# -----------------------------------------------------------------------
def scan_signals(daily_data: pd.DataFrame) -> list[dict]:
    """
    全銘柄の株価データからテクニカルシグナルをスキャンし、
    条件を満たした銘柄について Gemini で企業調査を行う。

    Parameters
    ----------
    daily_data : pd.DataFrame
        DBManager.load_analysis_data() が返す DataFrame。
        カラム: ticker, date, open, high, low, price, volume

    Returns
    -------
    list[dict]
        シグナルが立った銘柄のリスト。各要素:
        {
            "ticker":      str,   # 例 "30480.T"
            "price":       float, # 最新終値
            "signal_type": str,   # シグナル種別
            "reason":      str,   # シグナルの根拠
            "insight":     str,   # Gemini の企業調査結果
        }
    """
    if daily_data.empty:
        print("⚠️ データが空です。シグナルスキャンをスキップします。")
        return []

    cfg = _load_config()

    # Gemini クライアント初期化
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY が設定されていません")
    genai.configure(api_key=api_key)
    client = genai.GenerativeModel  # モデル呼び出しは _research_company 内で行う

    # google-generativeai の新 SDK に対応
    import google.generativeai as genai2
    genai2.configure(api_key=api_key)

    class _Client:
        """generate_content を統一インターフェースで呼ぶラッパー"""
        def __init__(self):
            self._model = genai2.GenerativeModel("gemini-2.0-flash")
        def models(self):
            return self
        def generate_content(self, model, contents):
            return self._model.generate_content(contents)

    gemini_client = _Client()

    # 銘柄ごとに分割してシグナル判定
    all_signals: list[dict] = []
    tickers = daily_data["ticker"].unique()
    print(f"🔍 シグナルスキャン開始: {len(tickers)} 銘柄")

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
        insight = _research_company(
            gemini_client,
            sig["ticker"],
            sig["signal_type"],
            sig["reason"],
        )
        results.append({
            "ticker":      sig["ticker"],
            "price":       sig["price"],
            "signal_type": sig["signal_type"],
            "reason":      sig["reason"],
            "insight":     insight,
        })
        print(f"  ✅ {sig['ticker']} ({sig['signal_type']}) 調査完了")

    return results
