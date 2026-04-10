"""
scoring_system.py
=================
YAML設定に基づき、銘柄の期待値を数値化する。
1. 出来高の勢い
2. 移動平均線への近さ（乖離率）
3. RSIの立ち上がり
4. 売買代金（流動性）
これらの要素を総合して 0〜100点 で評価する。
"""

import pandas as pd

def _linear_scale(val, min_val, max_val, score_max):
    """値を 0 〜 score_max の範囲に線形マッピングする補助関数"""
    if max_val <= min_val: return 0.0
    ratio = (val - min_val) / (max_val - min_val)
    return max(0.0, min(float(score_max), ratio * score_max))

# scoring_system.py の calculate_score() 内、流動性スコア部分を修正
def calculate_score(row: pd.Series, scoring_cfg: dict = None) -> float:
    cfg = scoring_cfg if scoring_cfg else {}
    weights = cfg.get('weights', {'volume_surge': 40, 'bias_proximity': 30, 'rsi_position': 20, 'liquidity_scale': 10})
    params = cfg.get('parameters', {'volume_max_multiplier': 5.0, 'bias_limit_pct': 10.0, 'turnover_ideal_min': 1000000000, 'rsi_ideal_range': [40, 65]})
    total_score = 0.0

    # 1. 出来高スコア
    v_ratio = row.get('volume_ratio', 1.0)
    total_score += _linear_scale(v_ratio, 1.0, params['volume_max_multiplier'], weights['volume_surge'])

    # 2. 25日線乖離率スコア
    bias = abs(row.get('mavg_25_diff', 0.0))
    bias_score = weights['bias_proximity'] - _linear_scale(bias, 0.0, params['bias_limit_pct'], weights['bias_proximity'])
    total_score += max(0.0, bias_score)

    # 3. RSIスコア
    rsi = row.get('rsi_14', 50.0)
    r_min, r_max = params['rsi_ideal_range']
    if r_min <= rsi <= r_max:
        total_score += weights['rsi_position']

    # 4. 流動性スコア ★ 修正：瞬間値 → 20日平均売買代金を優先使用
    turnover = row.get('turnover_avg_20', None)
    if turnover is None or turnover == 0:
        # turnover_avg_20 がない場合のフォールバック（後方互換）
        turnover = float(row.get('price', 0)) * float(row.get('volume', 0))
    total_score += _linear_scale(float(turnover), 0, params['turnover_ideal_min'], weights['liquidity_scale'])

    return float(round(total_score, 2))

if __name__ == "__main__":
    # モックデータによるテスト
    test_cfg = {
        'weights': {'volume_surge': 40, 'bias_proximity': 30, 'rsi_position': 20, 'liquidity_scale': 10},
        'parameters': {
            'volume_max_multiplier': 5.0,
            'bias_limit_pct': 10.0,
            'turnover_ideal_min': 1000000000,
            'rsi_ideal_range': [40, 65]
        }
    }

    test_cases = [
        {
            'name': '理想的な初動',
            'volume_ratio': 3.5,
            'mavg_25_diff': 1.5,
            'rsi_14': 55.0,
            'price': 2000,
            'volume': 600000  # 代金12億
        },
        {
            'name': '過熱・乖離大',
            'volume_ratio': 1.2,
            'mavg_25_diff': 12.0,
            'rsi_14': 80.0,
            'price': 5000,
            'volume': 100000  # 代金5億
        }
    ]

    print(f"{'ケース':<12} | {'総合スコア':<6}")
    print("-" * 25)
    for case in test_cases:
        s = pd.Series(case)
        score = calculate_score(s, test_cfg)
        print(f"{case['name']:<12} | {score:>6.2f}点")
