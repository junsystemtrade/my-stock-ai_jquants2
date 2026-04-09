import yfinance as yf
import pandas as pd

def is_market_crashing(config: dict):
    """
    前日の市場（日経平均等）が閾値を超えて下落しているか判定する。
    main.py の STEP 2.5 とロジックを共通化できる構造に整理。
    """
    breaker_cfg = config.get('filter', {}).get('market_breaker', {})
    if not breaker_cfg.get('enabled', False):
        return False, 0.0

    symbol = breaker_cfg.get('symbol', '^N225')
    threshold = breaker_cfg.get('drop_threshold_pct', -1.5)

    try:
        # 余裕を持って5日分取得（祝日や休場を考慮）
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="5d")
        
        if len(df) < 2:
            return False, 0.0

        # 直近2行の終値から騰落率を計算
        # pct_change() を使うと1行で記述可能
        change_pct = df['Close'].pct_change().iloc[-1] * 100

        # 判定
        return (change_pct <= threshold), round(change_pct, 2)

    except Exception as e:
        # ログ出力は main 側に任せるか、最小限にする
        print(f"⚠️ {symbol} データ取得失敗: {e}")
        return False, 0.0
