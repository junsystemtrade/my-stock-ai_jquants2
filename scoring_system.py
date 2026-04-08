"""
scoring_system.py
=================
シグナル検知された銘柄に対し、多角的なテクニカル指標から「投資期待値」をスコアリングする。

設計方針:
  - 基礎点を 50点 とし、ポジティブ要素で加点、ネガティブ要素で減点。
  - 地合い（25日線）、勢い（出来高）、リスク（急落・過熱）を数値化。
  - バックテストで「高スコアほど高勝率」となるようなエッジを目指す。
"""

import pandas as pd

def calculate_score(row: pd.Series, scoring_cfg: dict = None) -> float:
    # 1. 基礎点を 40.0 に引き下げ（ここから這い上がる銘柄を探す）
    score = 40.0

    # 2. 中期トレンド加点（最大 +20点）
    if row.get('is_above_ma25', False):
        score += 10.0
        # 25日線が上向きならさらに加速
        if row.get('ma25_upward', False):
            score += 10.0
    else:
        score -= 10.0

    # 3. 出来高の爆発力（最大 +30点）※ここを最も重視
    v_ratio = row.get('volume_ratio', 1.0)
    if v_ratio >= 3.0:
        score += 30.0  # 圧倒的な買い
    elif v_ratio >= 2.0:
        score += 20.0
    elif v_ratio >= 1.5:
        score += 10.0
    elif v_ratio < 0.5:
        score -= 10.0

    # 4. エントリー位置の最適化（最大 +10点 / 最小 -20点）
    diff5 = row.get('mavg_5_diff', 0.0)
    if 0.5 <= diff5 <= 3.5:
        score += 10.0  # 理想的な初動
    elif diff5 > 8.0:
        score -= 20.0  # 飛びつき買い厳禁（過熱）

    # 5. RSIの「余白」評価（最大 +10点 / 最小 -20点）
    rsi = row.get('rsi_14', 50.0)
    if 50.0 <= rsi <= 65.0:
        score += 10.0  # 上昇余力たっぷり
    elif rsi > 75.0:
        score -= 20.0  # 天井圏リスク

    return float(max(0.0, min(100.0, score)))

if __name__ == "__main__":
    # テスト用ダミーデータ
    test_row = pd.Series({
        'is_above_ma25': True,
        'volume_ratio': 2.5,
        'mavg_5_diff': 1.0,
        'rsi_14': 55.0
    })
    print(f"Test Score: {calculate_score(test_row)}")
