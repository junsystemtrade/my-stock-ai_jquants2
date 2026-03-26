import os
import google.generativeai as genai

def get_gemini_analysis(ticker, price, score):
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key: return "⚠️ APIキー未設定"
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    
    prompt = f"銘柄「{ticker}」を分析。株価:{int(price)}円, スコア:{score}/5\n【事業内容】15字以内\n【注目理由】30字以内\n【投資判断】50字以内"
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except: return "⚠️ AI分析エラー"

def scan_signals(data_dict):
    results = []
    for ticker, df in data_dict.items():
        if len(df) < 75: continue
        df['ma25'] = df['close'].rolling(25).mean()
        df['ma75'] = df['close'].rolling(75).mean()
        curr = df.iloc[-1]
        score = 0
        if curr['ma25'] > curr['ma75']: score += 3
        if curr['close'] > curr['ma25']: score += 2
        if score >= 5:
            insight = get_gemini_analysis(ticker, curr['close'], score)
            results.append({"ticker": ticker, "price": curr['close'], "score": score, "insight": insight})
    return results
