"""
main.py
=======
クォータ対策 (429 RESOURCE_EXHAUSTED) を強化した最新安定版。
google-genai SDK と gemini-1.5-flash を使用。
JQuants APIによる銘柄名取得機能を追加。
"""

import os
import time
import functools
import requests
import pandas as pd
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
# JQuants API 認証・銘柄名取得
# -----------------------------------------------------------------------
@functools.lru_cache(maxsize=1)
def _get_jquants_access_token() -> str | None:
    """
    リフレッシュトークンからアクセストークンを取得。
    lru_cacheにより1プロセス中は1回だけ実行される。
    """
    refresh_token = os.getenv("JQUANTS_API_KEY", "").strip()
    if not refresh_token:
        print("⚠️ JQUANTS_API_KEY が未設定です")
        return None
    try:
        res = requests.post(
            "https://api.jquants.com/v1/token/auth_refresh",
            params={"refreshtoken": refresh_token},
            timeout=10
        )
        res.raise_for_status()
        token = res.json().get("idToken")
        if token:
            print("✅ JQuantsアクセストークン取得成功")
        return token
    except Exception as e:
        print(f"⚠️ JQuantsトークン取得失敗: {e}")
        return None


def _get_company_name_from_jquants(code: str) -> str | None:
    """JQuants APIから銘柄名を取得。失敗時はNoneを返す。"""
    token = _get_jquants_access_token()
    if not token:
        return None
    try:
        res = requests.get(
            "https://api.jquants.com/v1/listed/info",
            headers={"Authorization": f"Bearer {token}"},
            params={"code": code},
            timeout=10
        )
        res.raise_for_status()
        data = res.json().get("info", [])
        if data:
            return data[0].get("CompanyName", None)
    except Exception as e:
        print(f"⚠️ JQuants銘柄名取得失敗 ({code}): {e}")
    return None


# -----------------------------------------------------------------------
# 企業リサーチ
# -----------------------------------------------------------------------
def get_detailed_research(ticker: str, signal_type: str, reason: str) -> tuple:
    """
    銘柄名はJQuants APIを優先取得。
    失敗時はGemini APIにフォールバック。
    """
    code = ticker.replace(".T", "")
    name, summary, topic = code, "（リサーチ制限中）", "⚠️ 情報を取得できませんでした。"

    # STEP1: JQuants APIで銘柄名を取得（高速・安定）
    jquants_name = _get_company_name_from_jquants(code)
    if jquants_name:
        name = jquants_name
        print(f"✅ JQuants銘柄名取得成功: {code} → {name}")

    if not client:
        return name, summary, "（APIキー未設定）"

    # STEP2: Geminiで事業概要とトピックを取得
    # 銘柄名が取れている場合はプロンプトを簡略化してトークン節約
    if jquants_name:
        prompt = f"企業名:{jquants_name}（銘柄コード{code}）について回答。1行目:【事業概要】(30字以内)、2行目:【最新トピック】(100字以内)。投資助言禁止。日本語必須。"
    else:
        prompt = f"銘柄コード{code}の日本企業について回答。1行目:企業名のみ、2行目:【事業概要】(30字以内)、3行目:【最新トピック】(100字以内)。投資助言禁止。日本語必須。"

    for attempt in range(3):
        try:
            time.sleep(attempt * 5)
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=prompt,
            )
            text = response.text.strip()
            lines = [line.strip() for line in text.split('\n') if line.strip()]

            if jquants_name:
                # 銘柄名確定済み：概要とトピックのみパース
                body = "\n".join(lines)
                if "【最新トピック】" in body:
                    parts = body.split("【最新トピック】")
                    summary = parts[0].replace("【事業概要】", "").replace(":", "").strip()
                    topic = parts[1].strip()
                elif len(lines) >= 2:
                    summary = lines[0].replace("【事業概要】", "").replace(":", "").strip()
                    topic = lines[1].replace("【最新トピック】", "").replace(":", "").strip()
                elif len(lines) == 1:
                    summary = lines[0].replace("【事業概要】", "").replace(":", "").strip()
            else:
                # 銘柄名未取得：従来通り3行パース
                if len(lines) >= 1:
                    name = lines[0].split(':', 1)[-1] if ':' in lines[0] else lines[0]
                    name = name.replace("1.", "").replace("**", "").strip()
                    body = "\n".join(lines[1:])
                    if "【最新トピック】" in body:
                        parts = body.split("【最新トピック】")
                        summary = parts[0].replace("【事業概要】", "").replace("2.", "").replace(":", "").strip()
                        topic = parts[1].replace("3.", "").replace(":", "").strip()
                    else:
                        summary = "（解析エラー：形式不一致）"
                        topic = body[:150]

            return name, summary, topic

        except Exception as e:
            print(f"⚠️ 銘柄{code} 試行{attempt+1}失敗: {e}")
            if "429" in str(e):
                time.sleep(20)
            continue

    return name, summary, topic


# -----------------------------------------------------------------------
# Discord通知
# -----------------------------------------------------------------------
def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ Discord Webhook URL 未設定")
        return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        try:
            requests.post(url, json={"content": chunk})
        except Exception as e:
            print(f"❌ Discord送信失敗: {e}")


# -----------------------------------------------------------------------
# メイン処理
# -----------------------------------------------------------------------
def main():
    print("🚀 システム起動: 安定リサーチモード")
    cfg = _load_config()

    # STEP 1-2: データ同期とロード
    try:
        portfolio_manager.sync_data()
        db = DBManager()
        daily_data = db.load_analysis_data(days=150)
    except Exception as e:
        print(f"❌ 初期エラー: {e}")
        return

    if daily_data.empty:
        print("⚠️ データなし")
        return

    # STEP 3: 市場環境
    market_change, market_status = _get_market_condition()

    # STEP 4: シグナルスキャン
    raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
    if not raw_signals:
        send_discord(f"📊 本日の地合い: **{market_status}**\n✅ スキャン完了：条件を満たす銘柄はありませんでした。")
        return

    # スコアリング
    scoring_cfg = cfg.get('scoring_logic', {})
    for s in raw_signals:
        s["score"] = calculate_score(pd.Series(s), scoring_cfg)

    # 上位3件に絞り込み
    signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    # STEP 5: レポート作成
    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 20 + "\n"

    for i, s in enumerate(signals, 1):
        print(f"⏳ 銘柄 {s['ticker']} リサーチ準備中...")
        time.sleep(15)

        name, business, topic = get_detailed_research(s['ticker'], s['signal_type'], s['reason'])

        report += (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"**{name}** ({s['ticker']} / {int(s['price']):,}円)\n"
            f"**事業概要**: {business}\n"
            f"**シグナル**: {s['signal_type']}\n"
            f"**根拠**: {s['reason']}\n"
            f"**スコア**: {s['score']:.1f}点\n" 
            f"**直近トピック**: {topic}\n"
            f"────────────────────\n"
        )

    send_discord(report)
    print("✅ 全プロセス正常終了")


if __name__ == "__main__":
    main()
