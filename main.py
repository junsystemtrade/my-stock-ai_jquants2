"""
main.py
=======
毎日の銘柄スキャン + Discord 通知のエントリーポイント。
GitHub Actions の daily workflow から呼ばれる。
"""

import os
import sys
import requests

import portfolio_manager
import signal_engine
from database_manager import DBManager


# -----------------------------------------------------------------------
# Discord 通知
# -----------------------------------------------------------------------
def send_discord(content: str):
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("⚠️ DISCORD_WEBHOOK_URL が未設定です。通知をスキップします。")
        return
    for i in range(0, len(content), 1990):
        chunk = content[i : i + 1990]
        res = requests.post(url, json={"content": chunk})
        if res.status_code not in (200, 204):
            print(f"❌ Discord 送信エラー: {res.status_code}")


# -----------------------------------------------------------------------
# メイン処理
# -----------------------------------------------------------------------
def main():
    print("🚀 システム起動: 株式分析プロセスを開始します")

    # STEP 1: DB を最新状態に差分同期
    print("\n--- STEP 1: データ同期 ---")
    try:
        portfolio_manager.sync_data()
    except Exception as e:
        msg = f"❌ データ同期エラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # STEP 2: 直近 150 日分のデータをロード
    print("\n--- STEP 2: データロード ---")
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)

    if daily_data.empty:
        msg = "⚠️ DBにデータがありません。先にバックフィルを実行してください。"
        print(msg)
        send_discord(msg)
        sys.exit(0)

    # STEP 3: シグナルスキャン + Gemini 企業調査
    print("\n--- STEP 3: シグナルスキャン ---")
    try:
        signals = signal_engine.scan_signals(daily_data)
    except Exception as e:
        msg = f"❌ シグナルスキャンエラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # STEP 4: Discord 通知
    print("\n--- STEP 4: Discord 通知 ---")
    if signals:
        report  = "🏛️ **【AI投資顧問：銘柄検知速報】**\n"
        report += f"📊 本日のシグナル検知: {len(signals)} 件\n"
        report += "━" * 20 + "\n"

        for s in signals:
            ticker       = s["ticker"]
            company_name = s.get("company_name", ticker.replace(".T", ""))
            price        = s["price"]
            signal_type  = s["signal_type"]
            reason       = s["reason"]
            business     = s.get("business", "")
            topic        = s.get("topic", "")
            context      = s.get("context", "")

            report += (
                f"📌 **{company_name}**（{ticker} / {int(price)}円）\n"
                f"🏢 事業概要: {business}\n"
                f"🔔 シグナル: {signal_type}\n"
                f"📐 根拠: {reason}\n"
            )
            if topic and topic != "特になし":
                report += f"📰 直近トピック: {topic}\n"
            if context:
                report += f"🔍 状況: {context}\n"
            report += "────────────────────\n"

        send_discord(report)
        print(f"✅ {len(signals)} 件のシグナルを Discord に送信しました")
    else:
        send_discord("✅ 本日のスキャン完了：条件に合致する銘柄はありませんでした。")
        print("✅ 本日はシグナルなし")

    print("\n✨ すべてのプロセスが正常に終了しました")


if __name__ == "__main__":
    main()
