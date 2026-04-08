# -----------------------------------------------------------------------
# 1 銘柄のバックテスト
# -----------------------------------------------------------------------
def _backtest_ticker(ticker: str, df: pd.DataFrame, cfg: dict, bt_params: dict) -> list[dict]:
    hold_days   = bt_params["hold_days"]
    stop_loss   = bt_params["stop_loss_pct"] / 100
    pos_size    = bt_params["position_size"]
    capital     = bt_params["initial_capital"]
    trades      = []
    in_position = False
    min_days    = cfg.get("filter", {}).get("min_data_days", 80)

    scoring_cfg = cfg.get('scoring_logic', {})

    for i in range(min_days, len(df)):
        # i番目（当日）のデータまででシグナル判定
        window_df = df.iloc[: i + 1].copy()

        if not in_position:
            hits = _check_signals(ticker, window_df, cfg)
            if not hits:
                continue
            
            # シグナルが出た翌営業日のデータが存在するか確認
            if i + 1 >= len(df):
                continue

            # --- エントリー: 翌日の「始値」で執行 ---
            entry_row   = df.iloc[i + 1]
            entry_date  = entry_row["date"]
            # 始値(open)を取得、なければprice(終値)で代用
            entry_price = float(entry_row["open"]) if pd.notna(entry_row["open"]) else float(entry_row["price"])
            
            if entry_price <= 0: continue

            # スコア算出（エントリー時のデータを使用）
            current_score = calculate_score(entry_row, scoring_cfg)

            signal_type = hits[0]["signal_type"]
            in_position = True

            exit_price  = None
            exit_date   = None
            exit_reason = "期間満了"

            # --- 保有期間中の監視（i+2営業日目から開始） ---
            # ルール: 前日の終値で損切り基準に達していたら、翌朝の「始値」で決済する
            for j in range(i + 2, min(i + 2 + hold_days, len(df))):
                prev_row = df.iloc[j-1] # 前日の結果
                curr_row = df.iloc[j]   # 当日の朝
                
                # 前日終値ベースで損切り判定
                prev_close = float(prev_row["price"])
                pnl_rate_at_close = (prev_close - entry_price) / entry_price
                
                if pnl_rate_at_close <= -stop_loss:
                    # 前日に損切りラインを割っていたので、当日の「始値」で成行決済
                    exit_price  = float(curr_row["open"]) if pd.notna(curr_row["open"]) else float(curr_row["price"])
                    exit_date   = curr_row["date"]
                    exit_reason = f"ストップロス(前日終値判定・当日始値決済)"
                    break

            # 損切りにかからず期間満了した場合
            if exit_price is None:
                # 指定日保持した後の翌営業日・始値で決済
                exit_idx = i + 1 + hold_days
                if exit_idx >= len(df):
                    exit_idx = len(df) - 1
                
                exit_row    = df.iloc[exit_idx]
                exit_price  = float(exit_row["open"]) if pd.notna(exit_row["open"]) else float(exit_row["price"])
                exit_date   = exit_row["date"]

            # 損益計算
            trade_capital = capital * pos_size
            pnl_pct        = (exit_price - entry_price) / entry_price * 100
            pnl_yen        = trade_capital * (pnl_pct / 100)

            trades.append({
                "ticker":      ticker,
                "score":       current_score,
                "signal_type": signal_type,
                "entry_date":  str(entry_date),
                "entry_price": round(entry_price, 2),
                "exit_date":   str(exit_date),
                "exit_price":  round(exit_price, 2),
                "pnl_pct":     round(pnl_pct, 2),
                "pnl_yen":     round(pnl_yen, 0),
                "exit_reason": exit_reason,
            })
            in_position = False

    return trades
