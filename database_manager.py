from sqlalchemy import create_engine
import os

class DBManager:
    def __init__(self):
        # GitHub SecretsからURLを取得
        db_url = os.getenv("DATABASE_URL")
        
        # ❗ここが重要：SupabaseのTransaction Mode(6543)でエラーを出さないための設定
        # ?prepare_threshold=0 を強制的に付与するか、接続オプションで無効化します
        if "6543" in db_url and "prepare_threshold" not in db_url:
            if "?" in db_url:
                db_url += "&prepare_threshold=0"
            else:
                db_url += "?prepare_threshold=0"
        
        self.engine = create_engine(
            db_url,
            # プールサーバー（6543）を使う際の推奨設定
            connect_args={"options": "-c statement_timeout=30000"} 
        )

    def save_prices(self, df):
        # 保存処理（前回と同じ）
        try:
            df.to_sql('daily_prices', self.engine, if_exists='append', index=False)
            print("✅ Supabaseへの保存が正常に完了しました！")
        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
