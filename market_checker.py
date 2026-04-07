import yfinance as yf
import pandas as pd

def is_market_crashing(config):
    """
    前日の日経平均が閾値を超えて下落しているか判定する
    """
    breaker_cfg = config.get('filter', {}).get('market_breaker', {})
    if not breaker_cfg.get('enabled', False):
        return False, 0.0

    symbol = breaker_cfg.get('symbol', '^N225')
    threshold = breaker_cfg.get('drop_threshold_pct', -1.5)

    try:
        # 直近2日分の終値を取得
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="2d")
        
        if len(df) < 2:
            return False, 0.0

        # 騰落率の計算
        prev_close = df['Close'].iloc[-2]
        last_close = df['Close'].iloc[-1]
        change_pct = ((last_close - prev_close) / prev_close) * 100

        # 判定
        is_crashing = change_pct <= threshold
        return is_crashing, round(change_pct, 2)

    except Exception as e:
        print(f"⚠️ 市場データ取得エラー: {e}")
        return False, 0.0
