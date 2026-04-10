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

    # ★ STEP 4: 手じまいシグナルチェック（買いスキャンより先に実行）
    exit_signals = signal_engine.check_exit_signals(daily_data)
    if exit_signals:
        exit_report = "🚨 **【手じまいシグナル】**\n"
        exit_report += "━" * 20 + "\n"
        for e in exit_signals:
            pnl_str = f"{e['pnl_pct']:+.2f}%"
            pnl_emoji = "📈" if e['pnl_pct'] >= 0 else "📉"
            exit_report += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"**{e['ticker']}**\n"
                f"**理由**: {e['exit_reason']}\n"
                f"**買値**: {int(e['entry_price']):,}円 → **現値**: {int(e['current_price']):,}円\n"
                f"**損益**: {pnl_emoji} {pnl_str}\n"
                f"**買いシグナル日**: {e['entry_date']}\n"
                f"────────────────────\n"
            )
        send_discord(exit_report)

    # STEP 5: 買いシグナルスキャン
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

    # STEP 6: 買いシグナルレポート作成
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

    # ★ STEP 7: 買いシグナル銘柄をDBに保存
    today = daily_data["date"].max()
    for s in signals:
        db.save_position(
            ticker=s["ticker"],
            entry_date=today,
            entry_price=s["price"],
            signal_type=s["signal_type"],
        )

    send_discord(report)
    print("✅ 全プロセス正常終了")
