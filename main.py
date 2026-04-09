"""
main.py
=======
毎日の銘柄スキャン + Discord 通知のエントリーポイント。
GitHub Actions の daily workflow から呼ばれる。
"""

import os
import sys
import requests
import pandas as pd

import portfolio_manager
import signal_engine
from database_manager import DBManager
from scoring_system import calculate_score
from signal_engine import _load_config, _get_market_condition


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
    
    # 0. 設定ロード
    cfg = _load_config()

    # 1. データ同期
    print("\n--- STEP 1: データ同期 ---")
    try:
        portfolio_manager.sync_data()
    except Exception as e:
        msg = f"❌ データ同期エラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # 2. データロード
    print("\n--- STEP 2: データロード ---")
    db = DBManager()
    daily_data = db.load_analysis_data(days=100)

    if daily_data.empty:
        msg = "⚠️ DBにデータがありません。先にバックフィルを実行してください。"
        print(msg)
        send_discord(msg)
        sys.exit(0)

    # 3. 市場環境チェック（ここでの判定を以降使い回す）
    print("\n--- STEP 3: 市場環境チェック ---")
    market_change, market_status = _get_market_condition()
    
    crash_threshold = cfg.get("filter", {}).get("market_breaker", {}).get("drop_threshold_pct", -2.0)
    print(f"市場ステータス: {market_status} (NIY=F: {market_change:+.2f}%)")

    if market_change <= crash_threshold:
        msg = (
            f"📉 **【市場警戒：スキャン停止】**\n"
            f"日経平均先物が大幅下落（{market_change:+.2f}%）したため、リスク回避として新規探索を中止しました。\n"
        )
        print(msg)
        send_discord(msg)
        return
    
    print("✅ 市場環境良好。スキャンを継続します。")

    # 4. シグナルスキャン & スコアリング
    print("\n--- STEP 4: シグナルスキャン & スコアリング ---")
    try:
        # market_status を引数として渡す（engine内での再計算を避ける）
        raw_signals = signal_engine.scan_signals(daily_data, market_status=market_status)
        
        if not raw_signals:
            send_discord("✅ 本日のスキャン完了：基準を満たす銘柄はありませんでした。")
            print("✅ シグナルなし")
            return

        # スコア計算
        scoring_cfg = cfg.get('scoring_logic', {})
        latest_rows_map = daily_data.sort_values("date").groupby('ticker').last().to_dict('index')
        
        for s in raw_signals:
            ticker = s["ticker"]
            row_data = latest_rows_map.get(ticker)
            if row_data:
                ticker_latest_row = pd.Series(row_data)
                ticker_latest_row['ticker'] = ticker
                ticker_latest_row['market_status'] = market_status
                s["score"] = calculate_score(ticker_latest_row, scoring_cfg)
            else:
                s["score"] = 0
        
        # スコア順に上位3つを抽出
        signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]

    except Exception as e:
        msg = f"❌ 解析プロセスエラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # 5. Discord 通知
    print("\n--- STEP 5: Discord 通知 ---")
    report = "🏛️ **【株式シグナル検知：厳選TOP3】**\n"
    report += f"📊 判定地合い: **{market_status}**\n"
    report += "━" * 15 + "\n"

    for i, s in enumerate(signals, 1):
        report += (
            f"{i}. **{s.get('company_name', s['ticker'].replace('.T', ''))}** ({s['ticker']})\n"
            f"   ⭐ **スコア: {s.get('score', 0)}点**\n"
            f"   💰 価格: {int(s['price']):,}円 | 🔔 {s['signal_type']}\n"
            f"   📐 根拠: {s['reason']}\n"
            f"────────────────────\n"
        )

    send_discord(report)
    print(f"✅ 上位 {len(signals)} 件を送信してプロセス正常終了")


if __name__ == "__main__":
    main()
