import os
import google.generativeai as genai

def get_gemini_analysis(ticker, price, score):
    """Gemini APIを使用して銘柄の事業内容と投資判断を生成"""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key: return "⚠️ APIキーが設定されていません"
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    
    # AIへの指示（プロンプト）
    prompt = (
        f"銘柄コード「{ticker}」についてプロの投資顧問として解説してください。\n"
        f"現在の株価は {int(price)}円、テクニカルスコアは {score}/5 です。\n\n"
        "以下の形式で簡潔に出力してください：\n"
        "【事業内容】15文字以内\n"
        "【注目ポイント】30文字以内\n"
        "【投資判断】50文字以内"
    )
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"⚠️ AI分析エラー: {e}"

def scan_signals(data_dict):
    """25日/75日移動平均線を使用して上昇トレンド銘柄を抽出"""
    results = []
    for ticker, df in data_dict.items():
        if len(df) < 75: continue
        
        # 移動平均線の計算
        df['ma25'] = df['close'].rolling(25).mean()
        df['ma75'] = df['close'].rolling(75).mean()
        curr = df.iloc[-1]
        
        score = 0
        # 判定ロジック1: ゴールデンクロス（短期が長期の上）
        if curr['ma25'] > curr['ma75']: score += 3
        # 判定ロジック2: 価格が短期線の上（強い上昇）
        if curr['close'] > curr['ma25']: score += 2
        
        # スコア満点（5点）の銘柄のみAI分析へ回す
        if score >= 5:
            insight = get_gemini_analysis(ticker, curr['close'], score)
            results.append({
                "ticker": ticker, 
                "price": curr['close'], 
                "score": score, 
                "insight": insight
            })
    return results
