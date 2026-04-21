# scoring_system.py: YAML設定に基づき銘柄を評価
import pandas as pd

def _linear_scale(val, min_val, max_val, score_max):
    if max_val <= min_val: return 0.0
    ratio = (val - min_val) / (max_val - min_val)
    return max(0.0, min(float(score_max), ratio * score_max))

def calculate_score(row: pd.Series, scoring_cfg: dict = None) -> float:
    # YAMLから設定を読み込み。なければ最新の精鋭設定を適用
    cfg = scoring_cfg if scoring_cfg else {}
    weights = cfg.get("weights", {"bias_proximity": 40, "rsi_position": 30, "liquidity_scale": 20, "volume_surge": 10})
    params = cfg.get("parameters", {
        "volume_max_multiplier": 5.0, "volume_penalty_above": 8.0,
        "bias_limit_pct": 10.0, "bias_ideal_min": 0.5, "bias_ideal_max": 4.0,
        "turnover_ideal_min": 3000000000, "rsi_ideal_range": [45, 60],
        "rsi_penalty_above": 70, "ma25_upward_bonus": 0.0
    })

    total_score = 0.0

    # 1. 出来高スコア（急増しすぎペナルティ）
    v_ratio = float(row.get("volume_ratio", 1.0))
    v_max, v_pen = float(params["volume_max_multiplier"]), float(params["volume_penalty_above"])
    w_vol = float(weights["volume_surge"])
    if v_ratio <= v_max: vol_score = _linear_scale(v_ratio, 1.0, v_max, w_vol)
    elif v_ratio <= v_pen: vol_score = w_vol
    else: vol_score = w_vol * (1.0 - 0.5 * min((v_ratio - v_pen) / v_pen, 1.0))
    total_score += vol_score

    # 2. 乖離率スコア（理想ゾーン 0.5-4.0%）
    bias = float(row.get("mavg_25_diff", 0.0))
    b_min, b_max, b_lim = params["bias_ideal_min"], params["bias_ideal_max"], params["bias_limit_pct"]
    w_bias = float(weights["bias_proximity"])
    if 0 <= bias < b_min: bias_score = w_bias * 0.5
    elif b_min <= bias <= b_max: bias_score = w_bias
    elif b_max < bias <= b_lim: bias_score = w_bias * (1.0 - (bias - b_max) / (b_lim - b_max))
    else: bias_score = 0.0
    total_score += bias_score

    # 3. RSIスコア（理想ゾーン 45-60）
    rsi = float(row.get("rsi_14", 50.0))
    r_min, r_max, r_pen = params["rsi_ideal_range"][0], params["rsi_ideal_range"][1], params["rsi_penalty_above"]
    w_rsi = float(weights["rsi_position"])
    if r_min <= rsi <= r_max: rsi_score = w_rsi
    elif rsi < r_min: rsi_score = w_rsi * 0.5 * (rsi / r_min)
    elif rsi <= r_pen: rsi_score = w_rsi * (1.0 - (rsi - r_max) / (r_pen - r_max))
    else: rsi_score = 0.0
    total_score += rsi_score

    # 4. 流動性スコア
    turnover = float(row.get("turnover_avg_20", 0))
    total_score += _linear_scale(turnover, 0, params["turnover_ideal_min"], weights["liquidity_scale"])

    return float(round(total_score, 2))
