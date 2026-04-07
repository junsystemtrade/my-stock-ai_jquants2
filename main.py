import os
import sys
import requests
import pandas as pd

import portfolio_manager
import signal_engine
from database_manager import DBManager
# --- 追加：スコアリングシステムの導入 ---
from scoring_system import calculate_score
from signal_engine import _load_config

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

def main():
    print("🚀 システム起動: 株式分析プロセスを開始します")

    # STEP 1: データ同期
    print("\n--- STEP 1: データ同期 ---")
    try:
        portfolio_manager.sync_data()
    except Exception as e:
        msg = f"❌ データ同期エラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # STEP 2: データロード
    print("\n--- STEP 2: データロード ---")
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)

    if daily_data.empty:
        msg = "⚠️ DBにデータがありません。先にバックフィルを実行してください。"
        print(msg)
        send_discord(msg)
        sys.exit(0)

    # STEP 3: シグナルスキャン
    print("\n--- STEP 3: シグナルスキャン ---")
    try:
        # まずは全候補を抽出
        raw_signals = signal_engine.scan_signals(daily_data)
        
        if raw_signals:
            # YAML設定からスコアリングロジックをロード
            cfg = _load_config()
            scoring_cfg = cfg.get('scoring_logic', {})
            
            # --- スコアリングの実行 ---
            for s in raw_signals:
                ticker = s["ticker"]
                # 当該銘柄の最新1日分データを取得してスコア計算
                ticker_latest_row = daily_data[daily_data['ticker'] == ticker].iloc[-1]
                s["score"] = calculate_score(ticker_latest_row, scoring_cfg)
            
            # スコア順にソートして上位3つを抽出
            signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]
            
            # ※ここで Gemini 調査関数を呼び出す（もし signal_engine 内でやっていない場合）
            # for s in signals:
            #     s.update(gemini_researcher.research(s["ticker"]))
        else:
            signals = []

    except Exception as e:
        msg = f"❌ シグナルスキャン/スコアリングエラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # STEP 4: Discord 通知
    print("\n--- STEP 4: Discord 通知 ---")
    if signals:
        report  = "🏛️ **【AI投資顧問：厳選TOP3】**\n"
        report += f"📊 本日の検知数: {len(raw_signals)} 件中、期待値トップ3をリサーチしました\n"
        report += "━" * 20 + "\n"

        for i, s in enumerate(signals, 1):
            ticker       = s["ticker"]
            company_name = s.get("company_name", ticker.replace(".T", ""))
            price        = s["price"]
            score        = s.get("score", 0)
            signal_type  = s["signal_type"]
            reason       = s["reason"]
            business     = s.get("business", "調査中...")
            topic        = s.get("topic", "")

            report += (
                f"🥇 **第{i}位: {company_name}**（{ticker}）\n"
                f"🔥 **Junスコア: {score}点**\n"
                f"💰 株価: {int(price)}円 | 🔔 {signal_type}\n"
                f"🏢 事業: {business}\n"
                f"📐 根拠: {reason}\n"
            )
            if topic and topic != "特になし":
                report += f"📰 トピック: {topic}\n"
            report += "────────────────────\n"

        send_discord(report)
        print(f"✅ スコア上位 {len(signals)} 件を Discord に送信しました")
    else:
        send_discord("✅ 本日のスキャン完了：条件に合致する銘柄はありませんでした。")
        print("✅ 本日はシグナルなし")

    print("\n✨ すべてのプロセスが正常に終了しました")

if __name__ == "__main__":
    main()
