from portfolio_manager import sync_data
from signal_engine import SignalEngine
import requests
import os

def notify_discord(message):
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url:
        return
    requests.post(url, json={"content": message})

def main():
    sync_data()

    engine = SignalEngine()
    result = engine.analyze()

    notify_discord(f"📢 AIシグナル\n{result}")

if __name__ == "__main__":
    main()
