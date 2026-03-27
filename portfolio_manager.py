import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from database_manager import DBManager

class PortfolioManager:
    def __init__(self):
        self.api_key = os.getenv("JQUANTS_API_KEY")
        if not self.api_key:
            raise ValueError("JQUANTS_API_KEY が設定されていません。")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

    def fetch_price(self, code, days=150):
        """REST API V2 で株価取得"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        url = (
            f"https://api.jpx-jquants.com/v2/prices/daily?"
            f"code={code}&from={start_date}&to={end_date}"
        )

        res = requests.get(url, headers=self.headers)
        data = res.json()

        if "daily_quotes" not in data:
            return pd.DataFrame()

        df = pd.DataFrame(data["daily_quotes"])
        df["ticker"] = code
        return df

    def sync_data(self):
        """DB に株価データを同期"""
        db = DBManager()

        # 例：TOPIX Core30 の一部
        tickers = ["7203", "6758", "9984", "9432", "8306"]

        for code in tickers:
            df = self.fetch_price(code)
            if df.empty:
                print(f"⚠ データなし: {code}")
                continue

            db.insert_price_data(df)
            print(f"📈 {code} をDBに保存")

        print("📊 J-Quants V2 → Supabase 同期完了")

def sync_data():
    pm = PortfolioManager()
    pm.sync_data()
