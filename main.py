import os
import portfolio_manager
import signal_engine
import backtest_engine
from database_manager import DBManager
import requests

def send_to_discord(content):
    """メインプロセス用の通知関数"""
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if url: requests.post(url, json={"content": content})

if __name__ == "__main__":
    print("🚀 システム起動: 株式分析プロセスを開始します")
    
    # STEP 1: Supabaseのデータを最新状態に更新
    portfolio_manager.sync_data()
    
    # STEP 2: 今日の判定用に直近150日分のデータをDBからロード
    db = DBManager()
    daily_data = db.load_analysis_data(days=150)
    
    # STEP 3: テクニカル指標とAIによる銘柄スキャン
    signals = signal_engine.scan_signals(daily_data)
    
    # STEP 4: 結果をDiscordへ通知
    if signals:
        report = f"🏛️ **【AI投資顧問：銘柄検知速報】**\n"
        for s in signals:
            report += f"📌 **{s['ticker']}** ({int(s['price'])}円)\n{s['insight']}\n────────────────────\n"
        send_to_discord(report)
    else:
        send_to_discord("✅ 本日のスキャン完了：条件に合致する銘柄はありませんでした。")
    
    # STEP 5: 5年間の長期バックテストを実行し、戦略の健全性をチェック
    print("📊 長期バックテストを開始します...")
    backtest_engine.run_backtest_and_report()
    
    print("✨ すべてのプロセスが正常に終了しました")
