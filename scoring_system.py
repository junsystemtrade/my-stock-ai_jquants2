import numpy as np

def calculate_score(ticker_row, scoring_cfg):
    """
    YAMLのscoring_logicに従って銘柄をスコアリングする
    """
    if not scoring_cfg.get('enabled', False):
        return 0
    
    weights = scoring_cfg['weights']
    params = scoring_cfg['parameters']
    
    # 1. 出来高スコア
    v_ratio = ticker_row.get('volume_surge_ratio', 1.0)
    score_vol = min(1.0, v_ratio / params['volume_max_multiplier']) * weights['volume_surge']
    
    # 2. 25日線乖離スコア (小さいほど良い)
    bias = abs(ticker_row.get('bias_25', 0))
    score_bias = max(0, 1.0 - (bias / params['bias_limit_pct'])) * weights['bias_proximity']
    
    # 3. RSIスコア (40-60の間を評価)
    rsi = ticker_row.get('rsi_14', 50)
    low, high = params['rsi_ideal_range']
    score_rsi = weights['rsi_position'] if low <= rsi <= high else 0
    
    # 4. 流動性スコア
    turnover = ticker_row.get('price', 0) * ticker_row.get('volume', 0)
    score_liq = min(1.0, turnover / params['turnover_ideal_min']) * weights['liquidity_scale']
    
    return round(score_vol + score_bias + score_rsi + score_liq, 1)
