import os
import redis
import json
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Configure Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Spark session initialization
spark = SparkSession.builder \
    .appName("PriceMonitor") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()

# Database connection settings
jdbc_url = f"jdbc:postgresql://{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{os.environ.get('DB_NAME')}"
connection_properties = {
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASSWORD'),
    "driver": "org.postgresql.Driver"
}


def get_price_deals(months, discount_percent):
    # Calculate the date range based on the user's input
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30) # assuming 30 days per month
    
    print(f" -> Getting historical prices from {start_date} to {end_date}")
    
    # Load price history data from the database
    price_history_df = spark.read.jdbc(
        url=jdbc_url,
        table="conta_verde.price_history",
        properties=connection_properties
    ).filter((F.col("recorded_at") >= start_date) & (F.col("recorded_at") <= end_date))

    price_history_df.show()

    print(f" -> Calculating deals with a discount of {discount_percent}%") 
    
    # Calculate the average prices of products within the specified period
    avg_prices_df = price_history_df.groupBy("product_id").agg(F.avg("price").alias("average_price"))
    
    # Load current products data
    products_df = spark.read.jdbc(
        url=jdbc_url,
        table="conta_verde.products",
        properties=connection_properties
    )
    
    # Determine the threshold prices and find deals
    threshold_prices_df = avg_prices_df.withColumn("threshold_price", F.col("average_price") * (1 - discount_percent / 100))
    
    deals_df = products_df.join(threshold_prices_df, products_df.id == threshold_prices_df.product_id) \
                          .filter(products_df.price < threshold_prices_df.threshold_price) \
                          .withColumn("price", F.col("price").cast("string")) \
                          .withColumn("average_price", F.col("average_price").cast("string")) \
                          .select(products_df.id, products_df.name, products_df.store_id, products_df.price, threshold_prices_df.average_price)
    
    
    deals_df.show()
    
    # Convert the result to a list of tuples
    deals = deals_df.collect()
    
    return deals


def handle_message(message):
    try:
        print(" -> Received message:", message)
        data = message['data']
        if data:
            job_data = json.loads(data)
            print(" <- Processing job:", job_data)
            deals = get_price_deals(job_data['time_window'], job_data['discount_percentage'])
            result = {
                'status': 'success',
                'task_name': "price_monitor_job_results",
                'deals': [{'id': deal.id, 'name': deal.name, 'store_id': deal.store_id, 'price': deal.price, 'average_price': deal.average_price} for deal in deals],
                'time_window': job_data['time_window'],
                'discount_percentage': job_data['discount_percentage'],
                'timestamp': datetime.now().isoformat()
            }
            redis_client.publish('price_monitor_job_results', json.dumps(result))
            print(" <- Published results:", result)
    except Exception as e:
        print(f"Error processing the message: {e}")

def subscribe_to_channel():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(**{'price_monitor_channel': handle_message})
    pubsub.run_in_thread(sleep_time=0.01)

if __name__ == '__main__':
    subscribe_to_channel()
    print("-- Price Monitor Service is running --")
    while True:
        pass