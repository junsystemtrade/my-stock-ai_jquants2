"""
scoring_system.py
=================
YAML設定に基づき、銘柄の期待値を数値化する。

【改善点】
1. RSIスコアを二値(on/off)→ 連続値に変更
2. 出来高スコアに「急増しすぎペナルティ」を追加
3. 乖離率スコアに方向性・理想ゾーンを追加
4. ma25_upward_bonus を0.0に変更（スコア帯ずれ修正）
   ※YAMLの ma25_upward_bonus で上書き可能
"""

import pandas as pd


def _linear_scale(val, min_val, max_val, score_max):
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
        "volume_penalty_above":  8.0,
        "bias_limit_pct":        10.0,
        "bias_ideal_min":        0.5,
        "bias_ideal_max":        5.0,
        "turnover_ideal_min":    1_000_000_000,
        "rsi_ideal_range":       [40, 65],
        "rsi_penalty_above":     72,
        "ma25_upward_bonus":     0.0,   # デフォルト0（スコアずれ防止）
    })

    total_score = 0.0

    # ------------------------------------------------------------------
    # 1. 出来高スコア（急増しすぎペナルティ付き）
    # ------------------------------------------------------------------
    v_ratio      = float(row.get("volume_ratio", 1.0))
    v_max        = float(params.get("volume_max_multiplier", 5.0))
    v_penalty_th = float(params.get("volume_penalty_above", 8.0))

    if v_ratio <= v_max:
        vol_score = _linear_scale(v_ratio, 1.0, v_max, weights["volume_surge"])
    elif v_ratio <= v_penalty_th:
        vol_score = float(weights["volume_surge"])
    else:
        over_ratio = min((v_ratio - v_penalty_th) / v_penalty_th, 1.0)
        vol_score  = float(weights["volume_surge"]) * (1.0 - 0.5 * over_ratio)

    total_score += vol_score

    # ------------------------------------------------------------------
    # 2. 乖離率スコア（理想ゾーン設定 + 方向性考慮）
    # ------------------------------------------------------------------
    bias_raw       = float(row.get("mavg_25_diff", 0.0))
    bias_ideal_min = float(params.get("bias_ideal_min", 0.5))
    bias_ideal_max = float(params.get("bias_ideal_max", 5.0))
    bias_limit     = float(params.get("bias_limit_pct", 10.0))
    w_bias         = float(weights["bias_proximity"])

    if bias_raw < 0:
        bias_score = 0.0
    elif bias_ideal_min <= bias_raw <= bias_ideal_max:
        bias_score = w_bias
    elif bias_raw < bias_ideal_min:
        bias_score = w_bias * 0.5
    elif bias_raw <= bias_limit:
        bias_score = w_bias * (1.0 - (bias_raw - bias_ideal_max) / (bias_limit - bias_ideal_max))
    else:
        bias_score = 0.0

    total_score += max(0.0, bias_score)

    # ------------------------------------------------------------------
    # 3. RSIスコア（連続値 + 過熱ペナルティ）
    # ------------------------------------------------------------------
    rsi          = float(row.get("rsi_14", 50.0))
    r_min, r_max = params.get("rsi_ideal_range", [40, 65])
    rsi_penalty  = float(params.get("rsi_penalty_above", 72))
    w_rsi        = float(weights["rsi_position"])

    if r_min <= rsi <= r_max:
        rsi_score = w_rsi
    elif rsi < r_min:
        rsi_score = w_rsi * 0.5 * (rsi / r_min)
    elif rsi <= rsi_penalty:
        rsi_score = w_rsi * (1.0 - (rsi - r_max) / (rsi_penalty - r_max))
    else:
        rsi_score = 0.0

    total_score += max(0.0, rsi_score)

    # ------------------------------------------------------------------
    # 4. 流動性スコア
    # ------------------------------------------------------------------
    turnover = row.get("turnover_avg_20", None)
    if turnover is None or turnover == 0:
        turnover = float(row.get("price", 0)) * float(row.get("volume", 0))
    total_score += _linear_scale(
        float(turnover), 0, params["turnover_ideal_min"], weights["liquidity_scale"]
    )

    # ------------------------------------------------------------------
    # 5. 25日線上向きボーナス（デフォルト0、YAMLで設定可能）
    # ------------------------------------------------------------------
    ma25_upward_bonus = float(params.get("ma25_upward_bonus", 0.0))
    if ma25_upward_bonus > 0 and bool(row.get("ma25_upward", False)):
        total_score += ma25_upward_bonus

    return float(round(total_score, 2))


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
            "ma25_upward_bonus":     0.0,
        },
    }

    test_cases = [
        {"name": "理想的な初動",          "volume_ratio": 3.5, "mavg_25_diff": 2.0, "rsi_14": 55.0, "turnover_avg_20": 1_500_000_000, "ma25_upward": True},
        {"name": "出来高急増しすぎ",       "volume_ratio": 12.0,"mavg_25_diff": 8.0, "rsi_14": 68.0, "turnover_avg_20": 2_000_000_000, "ma25_upward": True},
        {"name": "過熱・乖離大",           "volume_ratio": 1.2, "mavg_25_diff": 12.0,"rsi_14": 80.0, "turnover_avg_20": 500_000_000,  "ma25_upward": False},
        {"name": "GC+出来高確認",          "volume_ratio": 2.0, "mavg_25_diff": 1.5, "rsi_14": 52.0, "turnover_avg_20": 800_000_000,  "ma25_upward": True},
    ]

    print(f"{'ケース':<20} | {'スコア':>8}")
    print("-" * 35)
    for case in test_cases:
        score = calculate_score(pd.Series(case), test_cfg)
        print(f"{case['name']:<20} | {score:>8.2f}点")
