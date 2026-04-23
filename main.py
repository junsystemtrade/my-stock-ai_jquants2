"""
main.py

クォータ対策 (429 RESOURCE_EXHAUSTED) を強化した最新安定版。
google-genai SDK と gemini-2.0-flash を使用。
JPXマスターによる銘柄名取得・1銘柄1通知方式に対応。

変更点:

- STEP 2 で update_entry_prices() を呼び出し（前日ポジションの始値を更新）
- STEP 8 で entry_price=None でポジション保存
  （6:30 時点では始値が未確定のため NULL 保存、翌日に始値を反映）
- 手じまい通知に銘柄名を追加
  “””

import os
import time
import requests
import pandas as pd
from google import genai

import portfolio_manager
import signal_engine
import backtest_engine
from database_manager import DBManager
from scoring_system import calculate_score
from signal_engine import _load_config, _get_market_condition

# — Gemini API 設定 —

GOOGLE_API_KEY = os.getenv(“GOOGLE_API_KEY”)
client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

# ———————————————————————–

# JPXマスターから銘柄名取得（キャッシュ付き）

# ———————————————————————–

_ticker_name_cache: dict = {}

def _load_ticker_names() -> dict:
global _ticker_name_cache
if _ticker_name_cache:
return _ticker_name_cache
try:
stock_map = portfolio_manager.get_target_tickers()
_ticker_name_cache = {
k.replace(”.T”, “”): v[“name”]
for k, v in stock_map.items()
}
print(f”✅ 銘柄名キャッシュ構築完了: {len(_ticker_name_cache)} 件”)
except Exception as e:
print(f”⚠️ 銘柄名キャッシュ構築失敗: {e}”)
return _ticker_name_cache

def _get_company_name(code: str) -> str | None:
names = _load_ticker_names()
return names.get(code, None)

# ———————————————————————–

# 企業リサーチ

# ———————————————————————–

def get_detailed_research(ticker: str, signal_type: str, reason: str) -> tuple:
“””
銘柄名はJPXマスターから取得。
事業概要・トピック・シグナル考察はGeminiで取得。
戻り値: (name, summary, topic, consideration)
“””
code          = ticker.replace(”.T”, “”)
name          = code
summary       = “（リサーチ制限中）”
topic         = “⚠️ 情報を取得できませんでした。”
consideration = “（取得できませんでした）”

```
jpx_name = _get_company_name(code)
if jpx_name:
    name = jpx_name
    print(f"✅ 銘柄名取得成功: {code} -> {name}")

if not client:
    return name, summary, topic, consideration

prompt = f"""企業名:{jpx_name or code}（銘柄コード{code}）について以下の形式で回答してください。
```

【事業概要】（300字以内）
【直近トピック】（800字以内、決算・新製品・業績修正・株価動向など具体的な内容を含めること）
【シグナルとの関連】シグナル「{signal_type}」根拠「{reason}」との関連を500字以内で詳しく考察
投資助言は禁止。日本語必須。”””

```
for attempt in range(3):
    try:
        time.sleep(attempt * 5)
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        text_body = response.text.strip()
        lines     = [line.strip() for line in text_body.split("\n") if line.strip()]
        body      = "\n".join(lines)

        if "【直近トピック】" in body and "【シグナルとの関連】" in body:
            parts1        = body.split("【直近トピック】")
            summary       = parts1[0].replace("【事業概要】", "").replace(":", "").strip()
            parts2        = parts1[1].split("【シグナルとの関連】")
            topic         = parts2[0].strip()
            consideration = parts2[1].strip()
        elif "【直近トピック】" in body:
            parts1  = body.split("【直近トピック】")
            summary = parts1[0].replace("【事業概要】", "").replace(":", "").strip()
            topic   = parts1[1].strip()
        else:
            summary = body[:300]

        return name, summary, topic, consideration

    except Exception as e:
        print(f"⚠️ 銘柄{code} 試行{attempt+1}失敗: {e}")
        if "429" in str(e):
            time.sleep(20)
        continue

return name, summary, topic, consideration
```

# ———————————————————————–

# Discord 通知

# ———————————————————————–

def send_discord(content: str):
url = os.getenv(“DISCORD_WEBHOOK_URL”, “”).strip()
if not url:
print(“⚠️ Discord Webhook URL 未設定”)
return
for i in range(0, len(content), 1990):
chunk = content[i: i + 1990]
try:
requests.post(url, json={“content”: chunk})
except Exception as e:
print(f”❌ Discord送信失敗: {e}”)

# ———————————————————————–

# メイン処理

# ———————————————————————–

def main():
print(“🚀 システム起動: 安定リサーチモード”)
cfg = _load_config()
db  = DBManager()

```
# ------------------------------------------------------------------
# STEP 1: データ同期（NIY=F 含む毎回必ず取得）
# ------------------------------------------------------------------
print("\n--- STEP 1: データ同期 ---")
try:
    portfolio_manager.sync_data()
except Exception as e:
    print(f"❌ 初期エラー: {e}")
    return

# ------------------------------------------------------------------
# STEP 2: 前日ポジションの entry_price を始値で更新
# ------------------------------------------------------------------
print("\n--- STEP 2: 前日ポジションの始値更新 ---")
try:
    updated = db.update_entry_prices()
    if updated > 0:
        print(f"✅ {updated} 件のポジションに始値を設定しました")
    else:
        print("ℹ️ 更新対象のポジションはありません")
except Exception as e:
    print(f"⚠️ 始値更新エラー（続行します）: {e}")

# ------------------------------------------------------------------
# STEP 3: データロード
# ------------------------------------------------------------------
print("\n--- STEP 3: データロード ---")
daily_data = db.load_analysis_data(days=150)
if daily_data.empty:
    print("⚠️ データなし")
    return

# ------------------------------------------------------------------
# STEP 4: 市場環境チェック
# ------------------------------------------------------------------
print("\n--- STEP 4: 市場環境チェック ---")
market_change, market_status = _get_market_condition()
crash_threshold = cfg["filter"].get("market_breaker", {}).get("drop_threshold_pct", -2.0)
print(f"市場ステータス: {market_status} (NIY=F: {market_change:+.2f}%)")

if market_change <= crash_threshold:
    msg = (
        f"📉 **【市場警戒：スキャン停止】**\n"
        f"日経平均先物が大幅下落（{market_change:+.2f}%）のためスキャンを中止しました。\n"
    )
    print(msg)
    send_discord(msg)
    return

print("✅ 市場環境良好。スキャンを継続します。")

# ------------------------------------------------------------------
# STEP 5: 手じまいシグナルチェック
# ------------------------------------------------------------------
print("\n--- STEP 5: 手じまいシグナルチェック ---")
_load_ticker_names()  # 銘柄名キャッシュを事前構築
exit_signals = signal_engine.check_exit_signals(daily_data)
if exit_signals:
    exit_report = "🚨 **【手じまいシグナル】**\n"
    for e in exit_signals:
        pnl_str     = f"{e['pnl_pct']:+.2f}%"
        pnl_emoji   = "📈" if e["pnl_pct"] >= 0 else "📉"
        code        = e["ticker"].replace(".T", "")
        company     = _get_company_name(code) or e["ticker"]
        exit_report += (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"**{company}**（{e['ticker']}）\n"
            f"**理由**: {e['exit_reason']}\n"
            f"**買値**: {int(e['entry_price']):,}円 → **現値**: {int(e['current_price']):,}円\n"
            f"**損益**: {pnl_emoji} {pnl_str}\n"
            f"**保有日数**: {e['held_days']}日\n"
            f"**買いシグナル日**: {e['entry_date']}\n"
            f"────────────────────\n"
        )
    send_discord(exit_report)

# ------------------------------------------------------------------
# STEP 6: 買いシグナルスキャン & スコアリング
# ------------------------------------------------------------------
print("\n--- STEP 6: シグナルスキャン & スコアリング ---")
raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
if not raw_signals:
    send_discord(
        f"📊 本日の地合い: **{market_status}**\n"
        f"✅ スキャン完了：条件を満たす銘柄はありませんでした。"
    )
    return

scoring_cfg = cfg.get("scoring_logic", {})
for s in raw_signals:
    s["score"] = calculate_score(pd.Series(s), scoring_cfg)

# 上位3件
signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

# ------------------------------------------------------------------
# STEP 7: Discord 通知（ヘッダー + 1銘柄1通知）
# ------------------------------------------------------------------
header = (
    f"🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    f"📊 判定地合い: **{market_status}**\n"
    f"{'━' * 20}"
)
send_discord(header)

for i, s in enumerate(signals, 1):
    print(f"⏳ 銘柄 {s['ticker']} リサーチ準備中...")
    time.sleep(15)

    name, business, topic, consideration = get_detailed_research(
        s["ticker"], s["signal_type"], s["reason"]
    )

    # 前日比計算
    df_ticker       = daily_data[daily_data["ticker"] == s["ticker"]].sort_values("date")
    prev_change_str = "（取得不可）"
    if len(df_ticker) >= 2:
        latest_price    = float(df_ticker.iloc[-1]["price"])
        prev_price      = float(df_ticker.iloc[-2]["price"])
        change_pct      = (latest_price - prev_price) / prev_price * 100
        change_yen      = latest_price - prev_price
        change_emoji    = "📈" if change_pct >= 0 else "📉"
        prev_change_str = f"{change_emoji} {change_yen:+.0f}円 ({change_pct:+.2f}%)"

    report = (
        f"**{i}/3銘柄目**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"**{name}**（{s['ticker']}）\n"
        f"**前日終値**: {int(s['price']):,}円  {prev_change_str}\n"
        f"**シグナル**: {s['signal_type']}\n"
        f"**根拠**: {s['reason']}\n"
        f"**スコア**: {s['score']:.1f}点\n"
        f"⏰ 始値（entry_price）は本日9:00以降に自動更新されます\n"
        f"────────────────────\n"
        f"📋 **事業概要**\n{business}\n"
        f"────────────────────\n"
        f"📰 **直近トピック**\n{topic}\n"
        f"────────────────────\n"
        f"🔍 **シグナルとの関連**\n{consideration}\n"
    )
    send_discord(report)

# ------------------------------------------------------------------
# STEP 8: ポジション保存（entry_price = None）
# ------------------------------------------------------------------
print("\n--- STEP 8: ポジション保存 (entry_price=None) ---")
today = daily_data["date"].max()
for s in signals:
    db.save_position(
        ticker      = s["ticker"],
        entry_date  = today,
        entry_price = None,          # 6:30 時点は始値未確定 → NULL で保存
        signal_type = s["signal_type"],
    )

print("✅ 全プロセス正常終了")
```

if **name** == “**main**”:
main()