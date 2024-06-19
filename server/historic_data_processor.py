import os
import time
import json
import redis
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window


# get the database secrets (environment variables)
POSTGREE_CREDENTIALS = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

# Create a spark session to read local files
def create_spark_session_local():
    print("="*5, "Creating spark session to read local files", "="*5)

    spark = SparkSession \
        .builder \
        .appName('Log_Spark_DF') \
        .getOrCreate()
    
    return spark


# Create a spark session to read postgres database
def create_spark_session_postgres():
    print("="*5, "Creating spark session to read postgres database", "="*5)

    while True:
        try:
            spark = SparkSession \
                .builder \
                .appName('PostgreSQL_Spark_DF') \
                .config("spark.jars", "postgresql-42.7.3.jar") \
                .getOrCreate()
            break
        except Exception as e:
            print("Not able to create the spark session: ", e)
            time.sleep(5)

    return spark


# get all files in mock\mock_files\log folder
def get_log_files(log_path):
    log_files = []

    # Get all files inside the folders in the log_path
    for root, _, files in os.walk(log_path):
        files = [root + "/" + file for file in files]
        log_files.extend(files)
    
    return log_files


#  Read all files and put them in the same spark dataframe
def read_files(spark, log_path):
    log_files = get_log_files(log_path)
    df = spark.read.option("delimiter", ";").option("header", True).csv(log_files[0])
    for file in log_files[1:]:
        df = df.union(spark.read.option("delimiter", ";").option("header", True).csv(file))
    print("="*5, f"The dataframe of {len(log_files)} files has been created", "="*5)

    # create a new column with the datetime
    df = df.withColumn('datetime', (F.col('timestamp') / 1e9).cast(TimestampType()))

    return df


# Process the df to get count of products bought per minute
def products_bought_minute(df_user_buy):
    # Count the number of products bought per minute per store
    df_stores = df_user_buy.groupBy(F.window('datetime', '1 minute'), 'store_id') \
        .count() \
        .orderBy('window', 'store_id')
    
    # Count the number of products bought per minute for all stores (store_id = All)
    df_all = df_user_buy.groupBy(F.window('datetime', '1 minute')) \
        .count() \
        .orderBy('window') \
        .withColumn('store_id', F.lit('All'))
    
    # Union the dataframes
    return df_stores.unionByName(df_all)


# Get the amount earned per minute
def amount_earned_minute(df_user_view, product_df):
    # Join the df with the product_df to get the price of the product 
    df_user_view = df_user_view.alias('df_user_view')
    df_user_view = df_user_view.join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")

    # Calculate the amount earned per minute per store
    df_store = df_user_view.groupBy(F.window('datetime', '1 minute'), 'df_user_view.store_id') \
        .agg(F.sum(F.col('price').cast('int')).alias('amount_earned')) \
        .orderBy('window', 'store_id')
        
    # Calculate the amount earned per minute for all stores (store_id = All)
    df_all = df_user_view.groupBy(F.window('datetime', '1 minute')) \
        .agg(F.sum(F.col('price').cast('int')).alias('amount_earned')) \
        .orderBy('window') \
        .withColumn('store_id', F.lit('All'))
    
    # Union the dataframes (by column names)
    return df_store.unionByName(df_all)


# Process the df to get the number of unique users that viewed a product per minute
def users_view_product_minute(df_user_view, product_df):
    # Join the df with the product_df to get the name of the product
    df_user_view = df_user_view.alias('df_user_view')
    df_user_view = df_user_view.join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")

    # Distinct count of users that viewed a product per minute per store
    df_store = df_user_view.groupBy(F.window('datetime', '1 minute'), 'name', 'df_user_view.store_id') \
        .agg(F.countDistinct('content').alias('unique_users'))

    # Distinct count of users that viewed a product per minute for all stores (store_id = All)
    df_all = df_user_view.groupBy(F.window('datetime', '1 minute'), 'name') \
        .agg(F.countDistinct('content').alias('unique_users')) \
        .withColumn('store_id', F.lit('All'))
    
    # Group the dataframe by window, store_id and unique_users
    return df_store.unionByName(df_all)


# Get ranking of the most viewed products per hour
def get_view_ranking_hour(df_user_view, product_df):
    # Join the df with the product_df to get the name of the product
    df_user_view = df_user_view.alias('df_user_view')
    df_user_view = df_user_view.join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")

    # Count the number of views of a product per hour per store
    df_store = df_user_view.groupBy(F.window('datetime', '1 hour'), 'name', 'df_user_view.store_id') \
        .agg(F.count('content').alias('views'))
    
    # Count the number of views of a product per hour for all stores (store_id = All)
    df_all = df_user_view.groupBy(F.window('datetime', '1 hour'), 'name') \
        .agg(F.count('content').alias('views')) \
        .withColumn('store_id', F.lit('All'))
    
    # Group the dataframe by window, store_id and views
    return df_store.unionByName(df_all)


# Get the median of number of views of a product before it was bought
def median_views_before_buy(df_user_view, df_user_buy):
    # Calculate number of views of a product per user
    df_user_view = df_user_view.groupBy('content', 'extra_2', 'store_id') \
        .agg(F.count('content').alias('views'))
    
    # For each product bought, get the number of views before it was bought for each user and fill the null values with 0
    df_user_buy = df_user_buy.alias('df_user_buy') \
        .join(df_user_view, ['content', 'extra_2', 'store_id'], 'left') \
        .na.fill({'views': 0})
    
    # Save the amount of buys that have the same number of views 
    df_store = df_user_buy.groupBy('views', 'df_user_buy.store_id') \
        .count() \
        .orderBy('df_user_buy.store_id', 'views')
    
    # Save the amount of buys that have the same number of views for all stores (store_id = All)
    df_all = df_user_buy.groupBy('views') \
        .count() \
        .orderBy('views') \
        .withColumn('store_id', F.lit('All'))

    return df_store.unionByName(df_all)


# Fuction to group the dataframe by a column as "key" and the other columns as "value" in a details column
def get_df_grouped(df, column_name, columns_list):
    df_grouped = df.groupBy(column_name).agg(
        F.collect_list(F.struct(*columns_list)).alias("details")
    )
    return df_grouped


# Function to obtain the last minute of the dataframe for each data in the column and group the column_list in a details column
def get_df_last_minute(df, window_column, column_name, columns_list):
    windowSpec = Window.partitionBy(column_name).orderBy(F.desc(window_column))
    df = df.withColumn(window_column, F.col(window_column).cast('string'))
    df = df.withColumn('rank', F.dense_rank().over(windowSpec)).filter(F.col('rank') == 1).drop('rank')

    return get_df_grouped(df, column_name, columns_list)


# Function to send the dataframe to the redis as a dictionary
def send_to_redis_as_dict(redis_client, df, task_name, column_name = 'store_id'):
    for row in df.collect():
        data = {}
        # Send 10 top products
        for i, detail in enumerate(row['details']):
            data[i] = {detail_key: detail_value for detail_key, detail_value in detail.asDict().items()}
            if i == 9:
                break
        redis_client.hset(task_name, str(row[column_name]), json.dumps(data))


# Create a spark session to read the postgres database
spark_post = create_spark_session_postgres()

# Read the log files
log_path = './mock_files/logs/'
spark_local = create_spark_session_local()
df = read_files(spark_local, log_path)
df.show()

jdbc_url = f"jdbc:postgresql://{POSTGREE_CREDENTIALS['host']}:{POSTGREE_CREDENTIALS['port']}/{POSTGREE_CREDENTIALS['dbname']}"

# Try to read the products table
while True:
    try:
        product_df = spark_post.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "conta_verde.products") \
            .option("user", POSTGREE_CREDENTIALS['user']) \
            .option("password", POSTGREE_CREDENTIALS['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        product_df = product_df.withColumn('store_id', F.col('store_id').cast('integer'))
        product_df.show()
        break
    except Exception as e:
        print("The products table isn't available yet: ", e)
        time.sleep(5)


print("="*5, "Processing the data", "="*5)

# Create a redis connection and flush the database
r = redis.Redis(host=os.environ.get('REDIS_HOST'), port=os.environ.get('REDIS_PORT'))
r.flushall()

# Filter the data to get only the buys and the views
df_user_buy = df.filter(df["type"] == "Audit").filter(df["extra_1"] == "BUY") # Get only the buys

df_user_view = df.filter(df["type"] == "User").filter(df["extra_1"] == "ZOOM") # Get only the views of the products
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', '')) # Remove the 'VIEW_PRODUCT ' from the extra_2 column
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', '\.$', '')) # Remove the final '.' from the extra_2 column


df_task1 = products_bought_minute(df_user_buy)
df_task1 = df_task1.withColumn('window_start', F.col('window')['start'].cast('string')) \
    .withColumn('window_end', F.col('window')['end']) \
    .drop('window')

print("="*5, "Número de produtos comprados por minuto:")
df_task1.show()

print("="*5, "ENVIANDO PARA O REDIS")

# Get the last minute of the dataframe for each store
df_task1_last_data = get_df_last_minute(df_task1, 'window_end', 'store_id', ['count', 'window_start', 'window_end'])
send_to_redis_as_dict(r, df_task1_last_data, 'purchases_per_minute')

# Testing redis
all_data = r.hgetall('purchases_per_minute')
print("Testing - Purchases per minute")
for key, value in all_data.items():
    print(key, json.loads(value))


df_task2 = amount_earned_minute(df_user_buy, product_df)
df_task2 = df_task2.withColumn('window_start', F.col('window')['start'].cast('string')) \
    .withColumn('window_end', F.col('window')['end']) \
    .drop('window')

print("="*5, "Valor faturado por minuto:")
df_task2.show()

print("="*5, "ENVIANDO PARA O REDIS")

# Obtain the last minute of the dataframe for each store
df_task2_last_data = get_df_last_minute(df_task2, 'window_end', 'store_id', ['amount_earned', 'window_start', 'window_end'])
send_to_redis_as_dict(r, df_task2_last_data, 'revenue_per_minute')

# Testing redis
all_data = r.hgetall('revenue_per_minute')
for key, value in all_data.items():
    print(key, json.loads(value))


df_task3 = users_view_product_minute(df_user_view, product_df)
df_task3 = df_task3.withColumn('window_start', F.col('window')['start'].cast('string')) \
    .withColumn('window_end', F.col('window')['end']) \
    .drop('window')

print("="*5, "Número de usuários únicos visualizando cada produto por minuto:")
df_task3.show()

print("="*5, "ENVIANDO PARA O REDIS")
df_task3_last_data = get_df_last_minute(df_task3, 'window_end', 'store_id', ['name', 'unique_users', 'window_start', 'window_end'])
send_to_redis_as_dict(r, df_task3_last_data, 'unique_users_per_minute')

# Testing redis
all_data = r.hgetall('unique_users_per_minute')
for key, value in all_data.items():
    print(key, json.loads(value))


df_task4 = get_view_ranking_hour(df_user_view, product_df)
df_task4 = df_task4.withColumn('window_start', F.col('window')['start'].cast('string')) \
    .withColumn('window_end', F.col('window')['end']) \
    .drop('window')

print("="*5, "Ranking dos produtos mais visualizados por hora:")
df_task4.show()

print("="*5, "ENVIANDO PARA O REDIS")
df_task4_last_data = get_df_last_minute(df_task4, 'window_end', 'store_id', ['name', 'views', 'window_start', 'window_end'])
send_to_redis_as_dict(r, df_task4_last_data, 'ranking_viewed_products_per_hour')

# Testing redis
all_data = r.hgetall('ranking_viewed_products_per_hour')
for key, value in all_data.items():
    print(key, json.loads(value))


# PRECISA CALCULAR A MEDIANA DESSES DADOS
df_task5 = median_views_before_buy(df_user_view, df_user_buy)
print("="*5, "Mediana do número de vezes que um usuário visualiza um produto antes de efetuar uma compra:")
df_task5.show()

print("="*5, "ENVIANDO PARA O REDIS")
df_task5_grouped = get_df_grouped(df_task5, 'store_id', ['views', 'count'])
send_to_redis_as_dict(r, df_task5_grouped, 'median_views_before_buy')

# Testing redis
all_data = r.hgetall('median_views_before_buy')
for key, value in all_data.items():
    print(key, json.loads(value))
