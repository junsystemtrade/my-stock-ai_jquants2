import os
import requests
import portfolio_manager
import signal_engine
import backtest_engine
from database_manager import DBManager

def send_to_discord(content):
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if url: requests.post(url, json={"content": content})

if __name__ == "__main__":
    # 1. データの同期
    portfolio_manager.sync_data()
    
    # 2. 分析データのロード
    db = DBManager()
    all_data = db.load_analysis_data(days=150)
    
    # 3. シグナルスキャン
    signals = signal_engine.scan_signals(all_data)
    
    # 4. 銘柄リサーチ結果の通知
    if signals:
        report = f"🏛️ **【AI投資顧問：銘柄検知速報】**\n"
        for s in signals:
            report += f"📌 **{s['ticker']}** ({int(s['price'])}円)\n{s['insight']}\n────────────────────\n"
        send_to_discord(report)
    
    # 5. バックテスト実行
    backtest_engine.run_backtest_and_report()
