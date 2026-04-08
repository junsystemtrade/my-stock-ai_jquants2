"""
scoring_system.py
=================
シグナル検知された銘柄に対し、線形補間（リニア）方式でスコアリングを行う。
固定値加点ではなく「度合い」を数値化することで、銘柄間の微細な勢いの差を可視化する。
"""

import pandas as pd

def calculate_score(row: pd.Series, scoring_cfg: dict = None) -> float:
    # 1. 基礎点（ここを起点に加減点）
    score = 40.0

    # 2. 中期トレンド加点（最大 +20点 / 最小 -10点）
    # トレンドの「質」を評価
    if row.get('is_above_ma25', False):
        score += 10.0
        if row.get('ma25_upward', False):
            score += 10.0
    else:
        score -= 10.0

    # 3. 出来高の爆発力（最大 +30点 / 最小 -10点）
    # 0.5倍(最悪) ～ 3.0倍(最高) の間でスコアを線形に割り振る
    v_ratio = row.get('volume_ratio', 1.0)
    # 計算式: (現在の値 - 最小値) / (最大値 - 最小値) * 配点
    vol_bonus = ((v_ratio - 0.5) / (3.0 - 0.5)) * 30.0
    # 範囲外をクリップし、-10点～+30点の範囲で加算
    score += max(-10.0, min(30.0, vol_bonus))

    # 4. エントリー位置の最適化（最大 +10点 / 最小 -20点）
    # 5日線乖離率(diff5)が「2.0%」の時を最高評価とし、離れるほど減点
    diff5 = row.get('mavg_5_diff', 0.0)
    if diff5 > 8.0:
        score -= 20.0  # 過熱時は大幅減点
    else:
        # 2.0%を頂点とした山なりのスコアリング
        # 2%から1%離れるごとに 3.0点 減点する
        dist_from_ideal = abs(diff5 - 2.0)
        entry_bonus = 10.0 - (dist_from_ideal * 4.0) 
        score += max(-10.0, entry_bonus)

    # 5. RSIの「勢いと余白」評価（最大 +10点 / 最小 -20点）
    rsi = row.get('rsi_14', 50.0)
    if rsi > 75.0:
        score -= 20.0  # 過熱
    elif rsi < 30.0:
        score -= 10.0  # 弱すぎ
    elif 40.0 <= rsi <= 65.0:
        # 40(0点) ～ 65(10点) の間で線形加点
        rsi_bonus = ((rsi - 40.0) / (65.0 - 40.0)) * 10.0
        score += max(0.0, rsi_bonus)

    # 最終スコアを 0.0 ～ 100.0 の間に収めて返す
    return float(max(0.0, min(100.0, score)))

if __name__ == "__main__":
    # 比較テスト
    test_cases = [
        {'name': '勢い弱め', 'is_above_ma25': True, 'volume_ratio': 1.6, 'mavg_5_diff': 0.5, 'rsi_14': 42.0},
        {'name': '理想的',   'is_above_ma25': True, 'ma25_upward': True, 'volume_ratio': 2.8, 'mavg_5_diff': 2.1, 'rsi_14': 60.0},
        {'name': '過熱気味', 'is_above_ma25': True, 'volume_ratio': 4.0, 'mavg_5_diff': 9.0, 'rsi_14': 80.0},
    ]
    
    for case in test_cases:
        s = pd.Series(case)
        print(f"{case['name']:<6}: {calculate_score(s):.2f}点")
