# -----------------------------------------------------------------------
# Gemini によるサマリー整形（Discord 向け）: 修正版
# -----------------------------------------------------------------------
def _format_report_with_gemini(summary: dict, all_trades: list[dict]) -> str:
    """
    情報を厳選して Gemini に渡し、無料枠の制限(429)を回避しつつ
    質の高いレポートを生成する。
    """
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        return _format_report_plain(summary)

    # 1. 情報を「精鋭」だけに絞り込む（トークン節約 & 429対策）
    # 損益上位5件と下位5件だけを抽出
    sorted_trades = sorted(all_trades, key=lambda x: x["pnl_pct"], reverse=True)
    top_5 = sorted_trades[:5]
    worst_5 = sorted_trades[-5:]

    def _fmt_list(trades):
        return "\n".join([
            f"- {t['ticker']}: {t['pnl_pct']:+.1f}% ({t['entry_date']}～{t['exit_date']}, {t['exit_reason']})"
            for t in trades
        ])

    top_str = _fmt_list(top_5)
    worst_str = _fmt_list(worst_5)

    genai.configure(api_key=api_key)
    # 無料枠なので、負荷の低い gemini-1.5-flash または最新の 2.0-flash を使用
    model = genai.GenerativeModel("gemini-2.0-flash")

    prompt = f"""
システムトレードのバックテスト結果を要約し、Discord用の投資レポートを作成してください。

【統計データ】
・総トレード数: {summary['total_trades']}回
・勝率: {summary['win_rate']}%
・合計損益: {summary['total_pnl_yen']:,}円
・プロフィットファクター: {summary['profit_factor']}
・最大ドローダウン: {summary['max_drawdown_pct']}%

【代表的なトレード例】
勝トレード上位:
{top_str}

負トレード下位:
{worst_str}

【依頼】
1. この数値から、現在の戦略が「どのような相場に強く、何が課題か」を200文字程度で客観的に分析してください。
2. 最後に、今後の改善に向けた一言アドバイスを添えてください。
3. 絵文字を使い、Discordで読みやすいMarkdown形式で出力してください。
※投資助言や将来予測は含めないでください。
"""

    try:
        # 安全策として、1分あたりの制限に配慮（もしループ内で呼ぶ場合は time.sleep が必要）
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        # 429エラーなどが出た場合は、数字だけのプレーンレポートに切り替え
        print(f"⚠️ Gemini 分析エラー (429等): {e}")
        return _format_report_plain(summary) + "\n(※AI分析は制限によりスキップされました)"
