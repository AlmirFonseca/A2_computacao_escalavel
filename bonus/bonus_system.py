from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
import json
import os

# Kafka configurations
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC')

# Get the database secrets (environment variables)
POSTGREE_CREDENTIALS = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

print("Initializing Spark session...")

# Get all jars inside jars folder
jars_folder = "./jars/"
jars = ",".join([os.path.join(jars_folder, f) for f in os.listdir(jars_folder) if f.endswith(".jar")])

# Spark Session to read data from Kafka
spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .config("spark.jars", jars) \
    .getOrCreate()

URL = f"jdbc:postgresql://{POSTGREE_CREDENTIALS['host']}:{POSTGREE_CREDENTIALS['port']}/{POSTGREE_CREDENTIALS['dbname']}"
USER = POSTGREE_CREDENTIALS['user']
PASSWORD = POSTGREE_CREDENTIALS['password']
DRIVER = "org.postgresql.Driver"


print("Reading data from Kafka...")


# Esquema dos dados
schema = "timestamp String, store_id String, type String, user_id String, action String, details String"


# Function that return a splitted column with given schema
def csv_to_columns(df, column, schema):
    return df.select(F.from_csv(column, schema, options={"sep": ";"}).alias("data")) \
        .select("data.*") \
        .withColumn('datetime', (F.col('timestamp') / 1e9).cast(TimestampType()))


# Function that filter the purchases and split the details column into product_id and price
def filter_purchases(df):
    return df.filter(F.col("details").startswith("BUY")) \
        .withColumn("details", F.split("details", "-")) \
        .withColumn("product_id", F.col("details")[1].cast("string")) \
        .withColumn("price", F.col("details")[2].cast("float")) \
        .drop("details")

X = 500 # Minimum amount spent in the last 10 minutes to be eligible
Y = 2000 # Minimum amount spent in the last 6 hours to be eligible

def process_batch(batch_df, batch_id):
    events_df = filter_purchases(batch_df)

    # Use a window to sum the prices of the products bought in the last 6 hours and in the last 10 minutes
    window_6h = F.window("datetime", "6 hours")
    window_10m = F.window("datetime", "10 minutes")

    # Sum the prices of the products bought in the last 6 hours and in the last 10 minutes
    purchases_6h = events_df.withWatermark("datetime", "6 hours") \
        .groupBy(window_6h, "user_id", "store_id") \
        .agg(F.sum("price").alias("total_6h"))

    purchases_10m = events_df.withWatermark("datetime", "10 minutes") \
        .groupBy(window_10m, "user_id", "store_id") \
        .agg(F.sum("price").alias("total_10m"))

    # Obtain the users who spent more than X in the last 10 minutes and more than Y in the last 6 hours
    eligible_users = purchases_6h.join(purchases_10m, ["user_id", "store_id"]) \
        .filter((F.col("total_6h") > Y) & (F.col("total_10m") > X)) \
        .select("user_id", "store_id")
    
    # Write the eligible users to the output topic
    eligible_users.select(F.to_json(F.struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .save()
    
    # Print the count of eligible users
    print(f"Number of eligible users: {eligible_users.count()} in batch {batch_id}")


def main():
    # Dummy DataFrame to control the rate of data generation
    rate_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()

    # Define the read operation from PostgreSQL inside foreachBatch
    def read_from_postgres(_, batch_id):
        jdbc_df = spark.read \
            .format("jdbc") \
            .option("url", URL) \
            .option("dbtable", "events") \
            .option("user", USER) \
            .option("password", PASSWORD) \
            .option("driver", DRIVER) \
            .load()

        # Process the batch of data
        csv_df = csv_to_columns(jdbc_df, "message", schema)
        process_batch(csv_df, batch_id)

    # Apply foreachBatch to the streaming DataFrame
    query = rate_df.writeStream \
        .foreachBatch(read_from_postgres) \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()