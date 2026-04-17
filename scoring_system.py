"""
scoring_system.py
=================
YAML設定に基づき、銘柄の期待値を数値化する。

【改善点】
1. RSIスコアを二値(on/off)→ 連続値に変更
   - 理想レンジ(40-65)で満点、外れるほど減点
   - RSI過熱(70+)はペナルティ
2. 出来高スコアに「急増しすぎペナルティ」を追加
   - 5倍超えは逆に減点（すでに出遅れのサイン）
3. 乖離率スコアに方向性を追加
   - 25日線を少し上回っている状態(+1〜+5%)を最高評価
   - 乖離が大きすぎる場合はペナルティ
4. 25日線の傾きボーナスを追加
   - ma25_upward=True の場合+ボーナス（上昇トレンド確認）
"""

import pandas as pd


def _linear_scale(val, min_val, max_val, score_max):
    """値を 0 〜 score_max の範囲に線形マッピングする補助関数"""
    if max_val <= min_val:
        return 0.0
    ratio = (val - min_val) / (max_val - min_val)
    return max(0.0, min(float(score_max), ratio * score_max))


def calculate_score(row: pd.Series, scoring_cfg: dict = None) -> float:
    cfg = scoring_cfg if scoring_cfg else {}
    weights = cfg.get("weights", {
        "volume_surge":    40,
        "bias_proximity":  30,
        "rsi_position":    20,
        "liquidity_scale": 10,
    })
    params = cfg.get("parameters", {
        "volume_max_multiplier": 5.0,
        "volume_penalty_above":  8.0,   # ★NEW: これ以上は減点開始
        "bias_limit_pct":        10.0,
        "bias_ideal_min":        0.5,   # ★NEW: 理想乖離率の下限
        "bias_ideal_max":        5.0,   # ★NEW: 理想乖離率の上限
        "turnover_ideal_min":    1_000_000_000,
        "rsi_ideal_range":       [40, 65],
        "rsi_penalty_above":     72,    # ★NEW: ここ以上は大きくペナルティ
        "ma25_upward_bonus":     5.0,   # ★NEW: 25日線上向きボーナス点
    })

    total_score = 0.0

    # ------------------------------------------------------------------
    # 1. 出来高スコア（改善：急増しすぎペナルティ追加）
    # ------------------------------------------------------------------
    v_ratio      = float(row.get("volume_ratio", 1.0))
    v_max        = float(params.get("volume_max_multiplier", 5.0))
    v_penalty_th = float(params.get("volume_penalty_above", 8.0))

    if v_ratio <= v_max:
        # 1倍〜v_max倍: 線形スケール
        vol_score = _linear_scale(v_ratio, 1.0, v_max, weights["volume_surge"])
    elif v_ratio <= v_penalty_th:
        # v_max〜v_penalty_th: 満点維持
        vol_score = float(weights["volume_surge"])
    else:
        # v_penalty_th超え: 急増しすぎペナルティ（線形で満点の50%まで減少）
        over_ratio = min((v_ratio - v_penalty_th) / v_penalty_th, 1.0)
        vol_score  = float(weights["volume_surge"]) * (1.0 - 0.5 * over_ratio)

    total_score += vol_score

    # ------------------------------------------------------------------
    # 2. 乖離率スコア（改善：理想ゾーン設定 + 方向性考慮）
    # ------------------------------------------------------------------
    bias_raw    = float(row.get("mavg_25_diff", 0.0))  # +がプラス乖離
    bias_ideal_min = float(params.get("bias_ideal_min", 0.5))
    bias_ideal_max = float(params.get("bias_ideal_max", 5.0))
    bias_limit  = float(params.get("bias_limit_pct", 10.0))
    w_bias      = float(weights["bias_proximity"])

    if bias_raw < 0:
        # 25日線より下 → 0点（require_above_ma25=trueと合わせて原則除外済みだが念のため）
        bias_score = 0.0
    elif bias_ideal_min <= bias_raw <= bias_ideal_max:
        # 理想ゾーン(+0.5%〜+5%): 満点
        bias_score = w_bias
    elif bias_raw < bias_ideal_min:
        # ほぼ25日線上: 半点
        bias_score = w_bias * 0.5
    elif bias_raw <= bias_limit:
        # 理想超え〜上限: 線形減少
        bias_score = w_bias * (1.0 - (bias_raw - bias_ideal_max) / (bias_limit - bias_ideal_max))
    else:
        # 上限超え: 0点
        bias_score = 0.0

    total_score += max(0.0, bias_score)

    # ------------------------------------------------------------------
    # 3. RSIスコア（改善：二値 → 連続値 + 過熱ペナルティ）
    # ------------------------------------------------------------------
    rsi          = float(row.get("rsi_14", 50.0))
    r_min, r_max = params.get("rsi_ideal_range", [40, 65])
    rsi_penalty  = float(params.get("rsi_penalty_above", 72))
    w_rsi        = float(weights["rsi_position"])

    if r_min <= rsi <= r_max:
        # 理想ゾーン: 満点
        rsi_score = w_rsi
    elif rsi < r_min:
        # 理想以下（売られすぎ方向）: 線形で半点まで
        rsi_score = w_rsi * 0.5 * (rsi / r_min)
    elif rsi <= rsi_penalty:
        # 理想超え〜ペナルティ閾値: 線形減少
        rsi_score = w_rsi * (1.0 - (rsi - r_max) / (rsi_penalty - r_max))
    else:
        # 過熱（72以上）: 0点
        rsi_score = 0.0

    total_score += max(0.0, rsi_score)

    # ------------------------------------------------------------------
    # 4. 流動性スコア（変更なし）
    # ------------------------------------------------------------------
    turnover = row.get("turnover_avg_20", None)
    if turnover is None or turnover == 0:
        turnover = float(row.get("price", 0)) * float(row.get("volume", 0))
    total_score += _linear_scale(
        float(turnover), 0, params["turnover_ideal_min"], weights["liquidity_scale"]
    )

    # ------------------------------------------------------------------
    # 5. ★NEW: 25日線上向きボーナス
    # ------------------------------------------------------------------
    ma25_upward_bonus = float(params.get("ma25_upward_bonus", 5.0))
    if bool(row.get("ma25_upward", False)):
        total_score += ma25_upward_bonus

    return float(round(total_score, 2))


# -----------------------------------------------------------------------
# テスト
# -----------------------------------------------------------------------
if __name__ == "__main__":
    test_cfg = {
        "weights": {
            "volume_surge":    40,
            "bias_proximity":  30,
            "rsi_position":    20,
            "liquidity_scale": 10,
        },
        "parameters": {
            "volume_max_multiplier": 5.0,
            "volume_penalty_above":  8.0,
            "bias_limit_pct":        10.0,
            "bias_ideal_min":        0.5,
            "bias_ideal_max":        5.0,
            "turnover_ideal_min":    1_000_000_000,
            "rsi_ideal_range":       [40, 65],
            "rsi_penalty_above":     72,
            "ma25_upward_bonus":     5.0,
        },
    }

    test_cases = [
        {
            "name":          "理想的な初動",
            "volume_ratio":  3.5,
            "mavg_25_diff":  2.0,
            "rsi_14":        55.0,
            "turnover_avg_20": 1_500_000_000,
            "ma25_upward":   True,
        },
        {
            "name":          "出来高急増しすぎ（初動済み）",
            "volume_ratio":  12.0,
            "mavg_25_diff":  8.0,
            "rsi_14":        68.0,
            "turnover_avg_20": 2_000_000_000,
            "ma25_upward":   True,
        },
        {
            "name":          "過熱・乖離大",
            "volume_ratio":  1.2,
            "mavg_25_diff":  12.0,
            "rsi_14":        80.0,
            "turnover_avg_20": 500_000_000,
            "ma25_upward":   False,
        },
        {
            "name":          "RSI売られすぎ反発初動",
            "volume_ratio":  2.5,
            "mavg_25_diff":  1.0,
            "rsi_14":        38.0,
            "turnover_avg_20": 800_000_000,
            "ma25_upward":   True,
        },
        {
            "name":          "旧95点超え相当（改善後は下がるはず）",
            "volume_ratio":  5.5,
            "mavg_25_diff":  0.3,
            "rsi_14":        62.0,
            "turnover_avg_20": 3_000_000_000,
            "ma25_upward":   True,
        },
    ]

    print(f"{'ケース':<28} | {'旧スコア相当':>10} | {'新スコア':>8}")
    print("-" * 55)
    for case in test_cases:
        s     = pd.Series(case)
        score = calculate_score(s, test_cfg)
        print(f"{case['name']:<28} | {'→':>10} | {score:>8.2f}点")
