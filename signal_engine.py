import pandas as pd
from database_manager import DBManager
from google import genai

class SignalEngine:
    def __init__(self):
        self.db = DBManager()
        self.client = genai.Client()

    def analyze(self):
        df = self.db.load_analysis_data()
        if df.empty:
            return "データがありません。"

        prompt = f"""
        以下の株価データを分析し、買い/売りシグナルを生成してください。
        {df.to_string()}
        """

        response = self.client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )

        return response.text
