import functions_framework
import json
import random
import time
from google.cloud import pubsub_v1
from datetime import datetime

# Configuration
PROJECT_ID = "<your-project-id>"
TOPIC_NAME = "stock-events"
SYMBOLS = ["GOOG", "AAPL", "MSFT", "AMZN"]

@functions_framework.http
def generate_stock_events(request):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    
    # Simulate stock events
    for symbol in SYMBOLS:
        event = {
            "symbol": symbol,
            "price": round(random.uniform(100, 200), 2),
            "volume": random.randint(50, 500),
            "timestamp": datetime.utcnow().isoformat()
        }
        # Publish to Pub/Sub
        data = json.dumps(event).encode("utf-8")
        publisher.publish(topic_path, data)
    
    return "Published stock events", 200