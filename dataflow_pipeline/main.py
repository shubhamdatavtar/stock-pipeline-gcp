import json
import time
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataflow_v1beta3 as dataflow
from collections import defaultdict
from datetime import datetime, timedelta

# Configuration
PROJECT_ID = "<your-project-id>"
SUBSCRIPTION_NAME = "stock-events-sub"
BUCKET_NAME = "<your-project-id>-stock-raw"
BQ_DATASET = "stock_data"
BQ_TABLE = "processed_stocks"
WINDOW_SIZE = 300  # 5 minutes in seconds

def process_stock_events():
    # Initialize clients
    subscriber = pubsub_v1.SubscriberClient()
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    
    # In-memory windowed state (for simplicity; production would use stateful processing)
    window_data = defaultdict(list)
    
    def callback(message):
        try:
            # Parse message
            data = json.loads(message.data.decode("utf-8"))
            symbol = data["symbol"]
            price = data["price"]
            volume = data["volume"]
            timestamp = datetime.fromisoformat(data["timestamp"])
            
            # Store raw event in Cloud Storage
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"raw/{symbol}/{timestamp.isoformat()}.json")
            blob.upload_from_string(json.dumps(data))
            
            # Add to windowed data
            window_data[symbol].append((timestamp, price, volume))
            
            # Clean up old data (older than 5 minutes)
            cutoff = timestamp - timedelta(seconds=WINDOW_SIZE)
            window_data[symbol] = [(t, p, v) for t, p, v in window_data[symbol] if t > cutoff]
            
            # Calculate metrics
            prices = [p for _, p, _ in window_data[symbol]]
            volumes = [v for _, _, v in window_data[symbol]]
            moving_avg = sum(prices) / len(prices) if prices else 0.0
            vwap = sum(p * v for _, p, v in window_data[symbol]) / sum(volumes) if volumes else 0.0
            
            # Prepare BigQuery row
            row = {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "timestamp": timestamp.isoformat(),
                "moving_avg_5min": moving_avg,
                "vwap": vwap
            }
            
            # Stream to BigQuery
            errors = bq_client.insert_rows_json(
                f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}", [row]
            )
            if errors:
                print(f"BigQuery errors: {errors}")
            
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()
    
    # Start streaming pull
    subscriber.subscribe(subscription_path, callback=callback)
    print("Started Dataflow pipeline")
    while True:
        time.sleep(60)  # Keep the process alive

if __name__ == "__main__":
    process_stock_events()