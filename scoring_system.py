"""
scoring_system.py
=================
シグナル検知された銘柄に対し、線形補間（リニア）方式でスコアリングを行う。
固定値加点ではなく「度合い」を数値化することで、銘柄間の微細な勢いの差を可視化する。
"""

import pandas as pd

def _linear_scale(val, min_val, max_val, score_range):
    """値を特定のスコア範囲に線形マッピングする補助関数"""
    ratio = (val - min_val) / (max_val - min_val)
    return max(min(score_range), min(max(score_range), ratio * (max(score_range) - min(score_range)) + min(score_range)))

def calculate_score(row: pd.Series, scoring_cfg: dict = None) -> float:
    # デフォルト設定（configから取得できない場合のバックアップ）
    cfg = scoring_cfg or {}
    
    # 1. 基礎点
    score = 40.0

    # 2. 中期トレンド評価 (最大 +20 / 最小 -10)
    if row.get('is_above_ma25', False):
        score += 10.0
        if row.get('ma25_upward', False):
            score += 10.0
    else:
        score -= 10.0

    # 3. 出来高の爆発力評価 (最大 +30 / 最小 -10)
    # 0.5倍(-10点) ～ 3.0倍(+30点) で線形割り振振
    v_ratio = row.get('volume_ratio', 1.0)
    vol_score = _linear_scale(v_ratio, 0.5, 3.0, (-10.0, 30.0))
    score += vol_score

    # 4. エントリー位置（乖離率）の評価 (最大 +10 / 最小 -20)
    # 5日線乖離率(diff5)が「2.0%」を頂点とする山なり評価
    diff5 = row.get('mavg_5_diff', 0.0)
    if diff5 > 8.0:
        score -= 20.0
    else:
        dist_from_ideal = abs(diff5 - 2.0)
        entry_bonus = 10.0 - (dist_from_ideal * 4.0) 
        score += max(-10.0, entry_bonus)

    # 5. RSIの「勢いと余白」評価 (最大 +10 / 最小 -20)
    rsi = row.get('rsi_14', 50.0)
    if rsi > 75.0:
        score -= 20.0
    elif rsi < 30.0:
        score -= 10.0
    elif 40.0 <= rsi <= 65.0:
        # 40(0点) ～ 65(10点) の間で加点
        score += _linear_scale(rsi, 40.0, 65.0, (0.0, 10.0))

    # 最終スコアを 0.0 ～ 100.0 に収める
    return float(round(max(0.0, min(100.0, score)), 2))

if __name__ == "__main__":
    # 比較テスト
    test_cases = [
        {'name': '勢い弱め', 'is_above_ma25': True,  'volume_ratio': 1.2, 'mavg_5_diff': 0.5, 'rsi_14': 42.0},
        {'name': '理想的',   'is_above_ma25': True,  'ma25_upward': True, 'volume_ratio': 2.8, 'mavg_5_diff': 2.1, 'rsi_14': 60.0},
        {'name': '過熱気味', 'is_above_ma25': True,  'volume_ratio': 4.5, 'mavg_5_diff': 9.2, 'rsi_14': 80.0},
        {'name': '底這い',   'is_above_ma25': False, 'volume_ratio': 0.6, 'mavg_5_diff': -1.0, 'rsi_14': 32.0},
    ]
    
    print(f"{'ケース':<8} | {'スコア':<6}")
    print("-" * 20)
    for case in test_cases:
        s = pd.Series(case)
        val = calculate_score(s)
        print(f"{case['name']:<8} | {val:>5.2f}点")
