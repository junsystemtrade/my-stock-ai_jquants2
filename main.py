"""
main.py
=======
google-genai SDK (Gemini 2.0 Flash) を使用。
signal_engine のリサーチロジックを統合し、詳細な日本語レポートを生成。
"""

import os
import sys
import requests
import pandas as pd
import yfinance as yf
from google import genai

import portfolio_manager
import signal_engine
import backtest_engine
from database_manager import DBManager
from scoring_system import calculate_score
from signal_engine import _load_config, _get_market_condition

# --- Gemini API 設定 ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

# -----------------------------------------------------------------------
# Gemini による詳細企業リサーチ
# -----------------------------------------------------------------------
def get_detailed_research(ticker: str, signal_type: str, reason: str) -> tuple:
    """
    以前の signal_engine.py のロジックを活用し、
    企業名、事業概要、最新トピックを Gemini から一括取得する。
    """
    code = ticker.replace(".T", "")
    
    # デフォルト値
    name = code
    summary = "（調査中）"
    topic = "（トピック取得エラー）"

    if not client:
        return name, summary, "（APIキー未設定）"

    # プロンプト（signal_engine のエッセンスを統合）
    prompt = f"""
あなたは企業リサーチアナリストです。以下の銘柄について、企業の基本情報を日本語で調査・整理してください。

銘柄コード: {code}
検知されたシグナル: {signal_type}
シグナル詳細: {reason}

【出力形式】
1行目: 正確な企業名
2行目: 【事業概要】（主力事業、業界ポジションを30文字程度で）
3行目以降: 【最新トピック】（IR、提携、業績、シグナルとの関連ファクトを100文字程度で）

※売買の予想・推奨は一切禁止。
"""

    try:
        # モデルは 2.0 Flash を使用
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        res_text = response.text.strip().split('\n')
        
        # 応答のパース（最低限の分割）
        if len(res_text) >= 1:
            name = res_text[0].strip()
        
        full_body = "\n".join(res_text[1:])
        if "【最新トピック】" in full_body:
            parts = full_body.split("【最新トピック】")
            summary = parts[0].replace("【事業概要】", "").strip()
            topic = parts[1].strip()
        else:
            topic = full_text

    except Exception as e:
        topic = f"（リサーチエラー: {e}）"

    return name, summary, topic

# -----------------------------------------------------------------------
# Discord 通知
# -----------------------------------------------------------------------
def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url: return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        requests.post(url, json={"content": chunk})

# -----------------------------------------------------------------------
# メイン処理
# -----------------------------------------------------------------------
def main():
    print("🚀 システム起動: 専門リサーチモード")
    cfg = _load_config()

    # STEP 1-2: データ同期とロード
    portfolio_manager.sync_data()
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)
    if daily_data.empty: return

    # STEP 3: 市場環境
    market_change, market_status = _get_market_condition()
    
    # STEP 4: シグナルスキャン
    raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
    if not raw_signals:
        send_discord(f"📊 地合い: {market_status}\n✅ 本日のスキャン完了：対象なし")
        return

    # スコアリング
    scoring_cfg = cfg.get('scoring_logic', {})
    for s in raw_signals:
        s["score"] = calculate_score(pd.Series(s), scoring_cfg)
    
    signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    # STEP 5: レポート作成
    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 20 + "\n"

    for i, s in enumerate(signals, 1):
        # 以前の signal_engine 風ロジックで Gemini に調査させる
        name, business, topic = get_detailed_research(s['ticker'], s['signal_type'], s['reason'])

        report += (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"**{name}** ({s['ticker']} / {int(s['price']):,}円)\n"
            f"**事業概要**: {business}\n"
            f"**シグナル**: {s['signal_type']}\n"
            f"**根拠**: {s['reason']}\n"
            f"**直近トピック**: {topic}\n"
            f"────────────────────\n"
        )

    send_discord(report)
    print("✅ プロセス正常終了")

if __name__ == "__main__":
    main()
