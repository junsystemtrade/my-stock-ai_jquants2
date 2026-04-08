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
import yfinance as yf

import portfolio_manager
import signal_engine
from database_manager import DBManager
from scoring_system import calculate_score
from signal_engine import _load_config


# -----------------------------------------------------------------------
# 地合いチェック（サーキットブレーカー）
# -----------------------------------------------------------------------
def check_market_health(cfg: dict) -> tuple[bool, float]:
    """
    今朝 06:00 までの日経平均先物（CME等）の動きを確認する。
    """
    breaker_cfg = cfg.get("filter", {}).get("market_breaker", {})
    if not breaker_cfg.get("enabled", False):
        return False, 0.0

    # 現物 (^N225) ではなく先物 (NIY=F) を使用
    symbol = breaker_cfg.get("symbol", "NIY=F") 
    threshold = breaker_cfg.get("drop_threshold_pct", -1.5)

    try:
        ticker_yf = yf.Ticker(symbol)
        # 先物はほぼ24時間動いているため、直近の終値と1日前の終値を比較
        hist = ticker_yf.history(period="2d")
        if len(hist) < 2:
            print(f"⚠️ 市場データ不足: {symbol}")
            return False, 0.0

        prev_close = hist["Close"].iloc[-2]
        last_close = hist["Close"].iloc[-1]
        change_pct = ((last_close - prev_close) / prev_close) * 100

        is_crashing = change_pct <= threshold
        return is_crashing, round(change_pct, 2)
    except Exception as e:
        print(f"⚠️ 市場データ取得失敗: {e}")
        return False, 0.0


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
    
    # 設定ファイルのロード
    cfg = _load_config()

    # STEP 1: DB を最新状態に差分同期
    print("\n--- STEP 1: データ同期 ---")
    try:
        portfolio_manager.sync_data()
    except Exception as e:
        msg = f"❌ データ同期エラー: {e}"
        print(msg)
        send_discord(msg)
        sys.exit(1)

    # STEP 2: データロード（高速化のため直近60日に制限）
    print("\n--- STEP 2: データロード ---")
    db = DBManager()
    daily_data = db.load_analysis_data(days=60)

    if daily_data.empty:
        msg = "⚠️ DBにデータがありません。先にバックフィルを実行してください。"
        print(msg)
        send_discord(msg)
        sys.exit(0)

    # STEP 2.5: 地合いチェック
    print("\n--- STEP 2.5: 市場環境チェック ---")
    is_crashing, change_pct = check_market_health(cfg)
    
    # 判定スコアを常にコンソールに表示
    market_status = "⚠️ 警戒 (暴落検知)" if is_crashing else "✅ 良好 (正常稼働)"
    print(f"市場ステータス: {market_status}")
    print(f"前日の市場騰落率: {change_pct:+.2f}%")

    if is_crashing:
        msg = (
            f"📉 **【市場警戒：スキャン停止】**\n"
            f"前日の市場が大幅下落（{change_pct:+.2f}%）したため、新規エントリー探索を中止しました。\n"
            f"リスク回避を優先し、地合いの回復を待ちます。"
        )
        print(msg)
        send_discord(msg)
        return  # 暴落時はここで終了
    
    # 良好な場合はログを残して STEP 3 へ継続
    print("市場環境は規定値内です。シグナルスキャンを継続します。")

    # STEP 3: シグナルスキャン & 高速スコアリング
    print("\n--- STEP 3: シグナルスキャン & スコアリング ---")
    try:
        raw_signals = signal_engine.scan_signals(daily_data)
        
        if raw_signals:
            scoring_cfg = cfg.get('scoring_logic', {})
            
            # --- 高速化ロジック：銘柄ごとの最新行を事前に辞書化 ---
            # 100万件近いデータからループ内で検索するのを避け、1回だけ抽出する
            daily_data = daily_data.sort_values("date")
            latest_rows_map = daily_data.groupby('ticker').last().to_dict('index')
            
            for s in raw_signals:
                ticker = s["ticker"]
                if ticker in latest_rows_map:
                    # 辞書の要素をSeriesに変換してスコア計算関数へ渡す
                    ticker_latest_row = pd.Series(latest_rows_map[ticker])
                    ticker_latest_row['ticker'] = ticker
                    s["score"] = calculate_score(ticker_latest_row, scoring_cfg)
                else:
                    s["score"] = 0
            
            # スコア順にソートして上位3つを抽出
            signals = sorted(raw_signals, key=lambda x: x.get("score", 0), reverse=True)[:3]
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
        report += f"📊 全 {len(raw_signals)} 件の検知銘柄から期待値トップ3を選出しました。\n"
        report += "━" * 20 + "\n"

        for i, s in enumerate(signals, 1):
            ticker       = s["ticker"]
            company_name = s.get("company_name", ticker.replace(".T", ""))
            price        = s["price"]
            score        = s.get("score", 0)
            signal_type  = s["signal_type"]
            reason       = s["reason"]

            report += (
                f"🥇 **第{i}位: {company_name}**（{ticker}）\n"
                f"🔥 **Junスコア: {score}点**\n"
                f"💰 株価: {int(price)}円 | 🔔 {signal_type}\n"
                f"📐 根拠: {reason}\n"
                f"────────────────────\n"
            )

        send_discord(report)
        print(f"✅ スコア上位 {len(signals)} 件を送信完了")
    else:
        send_discord("✅ 本日のスキャン完了：条件に合致する銘柄はありませんでした。")
        print("✅ 本日はシグナルなし")

    print("\n✨ プロセス正常終了")


if __name__ == "__main__":
    main()
