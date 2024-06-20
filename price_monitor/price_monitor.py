import os
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


# Spark session initialization
spark = SparkSession.builder \
    .appName("PriceMonitor") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()


# Database connection settings
conn_params = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

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
    start_date = end_date - timedelta(days=30 * months)
    
    # Load price history data from the database
    price_history_df = spark.read.jdbc(
        url=jdbc_url,
        table="conta_verde.price_history",
        properties=connection_properties
    ).filter((F.col("recorded_at") >= start_date) & (F.col("recorded_at") <= end_date))
    
    
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
                          .select(products_df.id, products_df.name, products_df.price)
    
    # Convert the result to a list of tuples
    deals = [(row.id, row.name, row.price) for row in deals_df.collect()]
    
    return deals


if __name__ == "__main__":
    # Prompt the user to enter the parameters
    months = int(input("Enter the number of months to consider: "))
    discount_percent = float(input("Enter the desired discount percentage: "))
    
    # Find products with significant discounts
    deals = get_price_deals(months, discount_percent)
    
    if deals:
        print(f"Products with prices significantly below average over the last {months} months:")
        for deal in deals:
            print(f"ID: {deal[0]}, Name: {deal[1]}, Current Price: R${deal[2]:.2f}")
    else:
        print("No products found matching the specified criteria.")
