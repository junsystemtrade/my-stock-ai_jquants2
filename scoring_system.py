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
    """
    銘柄の各種指標（row内）に基づき、0〜100点のスコアを算出する。
    """
    # 1. 基礎点（ここからスタート）
    score = 50.0

    # 2. 中期トレンド（地合い）の判定
    # 銘柄が25日移動平均線より上にいるか（signal_engineで計算済み）
    is_above_ma25 = row.get('is_above_ma25', False)
    if is_above_ma25:
        score += 10.0  # 上昇トレンドへの追随
    else:
        score -= 10.0  # 逆張りリスク（減点）

    # 3. 出来高の勢い（モメンタム）
    # 5日平均に対する当日出来高の倍率
    v_ratio = row.get('volume_ratio', 1.0)
    if v_ratio >= 3.0:
        score += 20.0  # 異常な注目度（非常に強い加点）
    elif v_ratio >= 2.0:
        score += 15.0
    elif v_ratio >= 1.5:
        score += 8.0
    elif v_ratio < 0.6:
        score -= 5.0   # 買い手不在

    # 4. 短期的な「下げすぎ」または「過熱」の判定
    # 5日線乖離率（mavg_5_diff）
    diff5 = row.get('mavg_5_diff', 0.0)
    if diff5 < -10.0:
        # 下げの勢いが強すぎる場合は、リバウンド狙いよりもトレンド崩壊とみなして減点
        score -= 15.0
    elif -5.0 <= diff5 <= 2.0:
        # 押し目、あるいは上昇の初動として理想的
        score += 7.0
    elif diff5 > 10.0:
        # 短期的な急騰による高値掴みリスク
        score -= 10.0

    # 5. オシレーター（RSI）による補正
    rsi = row.get('rsi_14', 50.0)
    if 45.0 <= rsi <= 65.0:
        # ほどよい上昇余力
        score += 5.0
    elif rsi > 75.0:
        # 買われすぎによる調整懸念
        score -= 10.0
    elif rsi < 25.0:
        # 極端な売られすぎ（反発期待はあるがリスクも高い）
        score -= 5.0

    # 6. スコアの正規化（0点〜100点に収める）
    final_score = float(max(0.0, min(100.0, score)))
    
    return round(final_score, 1)

if __name__ == "__main__":
    # テスト用ダミーデータ
    test_row = pd.Series({
        'is_above_ma25': True,
        'volume_ratio': 2.5,
        'mavg_5_diff': 1.0,
        'rsi_14': 55.0
    })
    print(f"Test Score: {calculate_score(test_row)}")
