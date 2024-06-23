import os
import time
import json
import redis
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window


# Get the database secrets (environment variables)
POSTGREE_CREDENTIALS = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

# Create a Spark session to read local files
def create_spark_session_local():
    print("="*5, "Creating Spark session to read local files", "="*5)
    return SparkSession.builder.appName('Log_Spark_DF').getOrCreate()


# Create a Spark session to read Postgres database
def create_spark_session_postgres():
    print("="*5, "Creating Spark session to read Postgres database", "="*5)
    while True:
        try:
            spark = SparkSession.builder \
                .appName('PostgreSQL_Spark_DF') \
                .config("spark.jars", "postgresql-42.7.3.jar") \
                .getOrCreate()
            return spark
        except Exception as e:
            print("Unable to create the Spark session: ", e)
            time.sleep(5)


# Get all files in the log folder, excluding already read files
def get_log_files(log_path, already_read_files=[]):
    log_files = []
    for root, _, files in os.walk(log_path):
        files = [os.path.join(root, file) for file in files if os.path.join(root, file) not in already_read_files]
        log_files.extend(files)

    # If there are no files to read, and none of the files have been read, raise an error
    if not log_files and not already_read_files:
        raise FileNotFoundError("No files found to read")

    print("="*5, f"Found {len(log_files)} files to read", "="*5)
    return log_files


# Read all files and combine them into a single Spark dataframe filtered
def read_files(spark, log_files, filter_list=[]):
    dfs = {}

    df = spark.read.option("delimiter", ";").option("header", True).csv(log_files[0])
    for filter_name, filter_type, filter_value in filter_list:
        dfs[filter_name] = df.filter((F.col('type') == filter_type) & (F.col('extra_1') == filter_value))

    for file in log_files[1:]:
        df = df.union(spark.read.option("delimiter", ";").option("header", True).csv(file))
        for filter_name, filter_type, filter_value in filter_list:
            dfs[filter_name] = dfs[filter_name].union(df.filter((F.col('type') == filter_type) & (F.col('extra_1') == filter_value)))

    for filter_name, filter_df in dfs.items():
        dfs[filter_name] = filter_df.withColumn('datetime', (F.col('timestamp') / 1e9).cast(TimestampType()))

    print("="*5, f"The dataframe of {len(log_files)} files has been created", "="*5)
    return dfs, log_files


# Function to split the window column into start and end columns, with start as a string
def split_window_column(df, window_column='window'):
    return df.withColumn('window_start', F.col(window_column)['start'].cast('string')) \
        .withColumn('window_end', F.col(window_column)['end']) \
        .drop(window_column)


# Count of products bought per minute
def products_bought_minute(df_user_buy):
    df_store = df_user_buy.groupBy(F.window('datetime', '1 minute'), 'store_id') \
        .count() \
        .orderBy('window', 'store_id')
    df_all = df_store.groupBy('window') \
        .agg(F.sum('count').alias('count')) \
        .withColumn('store_id', F.lit('All'))  
    
    return split_window_column(df_store.unionByName(df_all))


# Amount earned per minute
def amount_earned_minute(df_user_view, product_df):
    df_user_view = df_user_view.alias('df_user_view') \
        .join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")
    
    df_store = df_user_view.groupBy(F.window('datetime', '1 minute'), 'df_user_view.store_id') \
        .agg(F.sum(F.col('price').cast('int')).alias('amount_earned')) \
        .orderBy('window', 'store_id')
    df_all = df_store.groupBy('window') \
        .agg(F.sum('amount_earned').alias('amount_earned')) \
        .withColumn('store_id', F.lit('All'))
    
    return split_window_column(df_store.unionByName(df_all))


# Number of unique users that viewed a product per minute
def users_view_product_minute(df_user_view, product_df):
    df_user_view = df_user_view.alias('df_user_view') \
        .join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")
    
    df_store = df_user_view.groupBy(F.window('datetime', '1 minute'), 'name', 'df_user_view.store_id') \
        .agg(F.countDistinct('content').alias('unique_users')) \
        .orderBy('window', 'store_id')
    df_all = df_store.groupBy('window', 'name') \
        .agg(F.sum('unique_users').alias('unique_users')) \
        .withColumn('store_id', F.lit('All'))
    
    return split_window_column(df_store.unionByName(df_all))


# Ranking of the most viewed products per hour
def get_view_ranking_hour(df_user_view, product_df):
    df_user_view = df_user_view.alias('df_user_view') \
        .join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")
    
    df_store = df_user_view.groupBy(F.window('datetime', '1 hour'), 'name', 'df_user_view.store_id') \
        .count() \
        .withColumnRenamed('count', 'views') \
        .orderBy('window', 'store_id', F.desc('views'))
    df_all = df_store.groupBy('window', 'name') \
        .agg(F.sum('views').alias('views')) \
        .withColumn('store_id', F.lit('All'))
    
    return split_window_column(df_store.unionByName(df_all))


# Median number of views before a product was bought
def median_views_before_buy(df_user_view, df_user_buy):
    df_user_view = df_user_view.groupBy('content', 'extra_2', 'store_id') \
        .count() \
        .withColumnRenamed('count', 'views')
    df_user_buy = df_user_buy.alias('df_user_buy') \
        .join(df_user_view, ['content', 'extra_2', 'store_id'], 'left') \
        .na.fill({'views': 0})
    
    df_store = df_user_buy.groupBy('views', 'df_user_buy.store_id') \
        .count() \
        .orderBy('store_id', 'views')

    df_all = df_store.groupBy('views') \
        .agg(F.sum('count').alias('count')) \
        .withColumn('store_id', F.lit('All'))

    return df_store.unionByName(df_all)


# Number of products sold without stock
def products_sold_without_stock(stock_df):
    df_sold_without_stock = stock_df.filter(F.col('quantity') < 0) \
        .groupBy('store_id') \
        .count() \
        .withColumnRenamed('count', 'products_sold_without_stock')
    df_all = df_sold_without_stock.groupBy() \
        .agg(F.sum('products_sold_without_stock').alias('products_sold_without_stock')) \
        .withColumn('store_id', F.lit('All'))
    
    return df_sold_without_stock.unionByName(df_all)


# Group the dataframe by a column as "key" and other columns as "value" in a details column
def get_df_grouped(df, column_name, columns_list):
     return df.groupBy(column_name) \
        .agg(F.collect_list(F.struct(*columns_list)).alias("details"))


# Separate the last time of the dataframe of the old for each data in the column 
def get_df_last_time(df, window_column, column_name):
    # Window specification to order by the window column
    windowSpec = Window.partitionBy(column_name) \
        .orderBy(F.desc(window_column))
    
    # Cast the window column to string and add a rank column to filter the last time
    df = df.withColumn(window_column, F.col(window_column).cast('string')) \
        .withColumn('rank', F.dense_rank().over(windowSpec))

    df_last = df.filter(F.col('rank') == 1) \
        .drop('rank')
    
    df_old = df.filter(F.col('rank') > 1) \
        .drop('rank')
    
    return df_last, df_old


# Save data in a file path
def save_data(df, file_path, overwrite=False):
    if overwrite:
        df.coalesce(1).write.option("header", "true").csv(file_path, mode='overwrite')
    else:
        df.coalesce(1).write.option("header", "true").csv(file_path, mode='append')


# Function to send the dataframe to Redis as a dictionary and publish the data
def send_to_redis_as_dict(redis_client, df, task_name, column_name='store_id', channel_name='ecommerce_data'):

    message = {}
    message['task_name'] = task_name
    for row in df.collect():

        store_id = row[column_name]

        data = {}
        # Send 10 top products
        for i, detail in enumerate(row['details']):
            data[i] = {detail_key: detail_value for detail_key, detail_value in detail.asDict().items()}
            if i == 9:
                break
        
        message[store_id] = data

        # Set the data in the Redis hash
        # redis_client.hset(task_name, str(store_id), json.dumps(data))
        
    # Publish the data to the Redis channel
    print("Sending message to Redis: ", message)
    redis_client.publish(channel_name, json.dumps(message))

    # Save the data as json in a file
    with open(f'./tasks/{task_name}.json', 'w') as f:
        json.dump(message, f)



# Merge two dataframes, grouping by specific columns, aggragating a column and ordering by columns
def merge_dataframes(df1, df2, group_columns, agg_column, order_columns):
    return df1.unionByName(df2) \
        .groupBy(*group_columns) \
        .agg(F.sum(agg_column).alias(agg_column)) \
        .orderBy(*order_columns, F.desc(agg_column))

# Function to execute a task
def execute_task(dfs, task_func, task_name):
    # Execute the task function
    print("="*5, f"Executing task {task_name}", "="*5)
    df_task = task_func(*dfs)

    return df_task


# Save the task in Redis and in a file
def save_task(df_task, tasks_path, task_name, column_list, redis_client, temporal=True):
    print("="*5, f"Saving task {task_name} in Redis and in a file", "="*5)

    if temporal:
        # Separate the last time of the dataframe of the old for each data in the column
        df_task_last, df_task_old = get_df_last_time(df_task, "window_end", "store_id")

    else:
        df_task_last = df_task
        df_task_old = df_task
    
    # Obtain a df in format to be sent to Redis
    df_task_grouped = get_df_grouped(df_task_last, "store_id", column_list)

    # Send the data to Redis
    send_to_redis_as_dict(redis_client, df_task_grouped, task_name)

    # Save the old data in a file
    # save_data(df_task_old, os.path.join(tasks_path, task_name), not temporal)

    return df_task_last



# Process all the data
def process_data(redis_client, spark_local, spark_post, log_files, tasks_path, df_tasks={}):
    URL = f"jdbc:postgresql://{POSTGREE_CREDENTIALS['host']}:{POSTGREE_CREDENTIALS['port']}/{POSTGREE_CREDENTIALS['dbname']}"
    USER = POSTGREE_CREDENTIALS['user']
    PASSWORD = POSTGREE_CREDENTIALS['password']
    DRIVER = "org.postgresql.Driver"

    dfs, files_already_read = read_files(spark_local, log_files, [("buy", "Audit", "BUY"), ("view", "User", "ZOOM")])

    print("="*5, "Processing the data", "="*5)

    df_user_buy = dfs["buy"]
    df_user_view = dfs["view"]
    df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', '')) # Remove the 'VIEW_PRODUCT ' from the extra_2 column
    df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', '\.$', '')) # Remove the final '.' from the extra_2 column

    product_df = spark_post.read.format("jdbc") \
        .option("url", URL).option("dbtable", "conta_verde.products") \
        .option("user", USER).option("password", PASSWORD) \
        .option("driver", DRIVER).load()
    
    stock_df = spark_post.read.format("jdbc") \
        .option("url", URL).option("dbtable", "conta_verde.stock") \
        .option("user", USER).option("password", PASSWORD) \
        .option("driver", DRIVER).load()


    # # ---------- Task 1 ----------
    df_task_1 = execute_task([df_user_buy], products_bought_minute, "purchases_per_minute")
    
    if df_tasks.get("purchases_per_minute"):
        df_task_1 = merge_dataframes(df_task_1, df_tasks["purchases_per_minute"], ['window_start', 'window_end', 'store_id'], 'count', ['window_end', 'store_id'])
    
    df_task_1 = save_task(df_task_1, tasks_path, "purchases_per_minute", ['count', 'window_start', 'window_end'], redis_client)
        

    # # ---------- Task 2 ----------
    df_task_2 = execute_task([df_user_view, product_df], amount_earned_minute, "revenue_per_minute")
    
    if df_tasks.get("revenue_per_minute"):
        df_task_2 = merge_dataframes(df_task_2, df_tasks["revenue_per_minute"], ['window_start', 'window_end', 'store_id'], 'amount_earned', ['window_end', 'store_id'])

    df_task_2 = save_task(df_task_2, tasks_path, "revenue_per_minute", ['amount_earned', 'window_start', 'window_end'], redis_client)


    # # ---------- Task 3 ----------
    df_task_3 = execute_task([df_user_view, product_df], users_view_product_minute, "unique_users_per_minute")
    
    if df_tasks.get("unique_users_per_minute"):
        df_task_3 = merge_dataframes(df_task_3, df_tasks["unique_users_per_minute"], ['window_start', 'window_end', 'name', 'store_id'], 'unique_users', ['window_end', 'store_id'])

    df_task_3 = save_task(df_task_3, tasks_path, "unique_users_per_minute", ['name', 'unique_users', 'window_start', 'window_end'], redis_client)


    # # ---------- Task 4 ----------
    df_task_4 = execute_task([df_user_view, product_df], get_view_ranking_hour, "ranking_viewed_products_per_hour")

    if df_tasks.get("ranking_viewed_products_per_hour"):
        df_task_4 = merge_dataframes(df_task_4, df_tasks["ranking_viewed_products_per_hour"], ['window_start', 'window_end', 'name', 'store_id'], 'views', ['window_end', 'store_id'])

    df_task_4 = save_task(df_task_4, tasks_path, "ranking_viewed_products_per_hour", ['name', 'views', 'window_start', 'window_end'], redis_client)

    
    # # ---------- Task 5 ----------
    df_task_5 = execute_task([df_user_view, df_user_buy], median_views_before_buy, "median_views_before_buy")
    
    if df_tasks.get("median_views_before_buy"):
        df_task_5 = merge_dataframes(df_task_5, df_tasks["task_5"], ['views', 'store_id'], 'count', ['store_id', 'views'])

    df_task_5 = save_task(df_task_5, tasks_path, "median_views_before_buy", ['count', 'views'], redis_client, temporal=False)


    # # ---------- Task 6 ----------
    df_task_6 = execute_task([stock_df], products_sold_without_stock, "without_stock")
    
    if df_tasks.get("without_stock"):
        df_task_6 = merge_dataframes(df_task_6, df_tasks["task_6"], ['store_id'], 'products_sold_without_stock', ['store_id'])

    df_task_6 = save_task(df_task_6, tasks_path, "without_stock", ['products_sold_without_stock'], redis_client, temporal=False)



    df_tasks = {
        "task_1": df_task_1,
        "task_2": df_task_2,
        "task_3": df_task_3,
        "task_4": df_task_4,
        "task_5": df_task_5,
        "task_6": df_task_6
    }

    print("="*5, "Data processed", "="*5)

    return df_tasks, files_already_read


if __name__ == "__main__":
    redis_host = os.environ.get('REDIS_HOST')
    redis_port = os.environ.get('REDIS_PORT')

    log_path = 'mock_files/logs/'
    tasks_path = 'tasks/'

    spark_post = create_spark_session_postgres()
    spark_local = create_spark_session_local()
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
    
    already_read_files = []
    df_tasks = {}

    while True:
        log_files = get_log_files(log_path, already_read_files)
        if log_files:
            df_tasks, already_read_files = process_data(redis_client, spark_local, spark_post, log_files, tasks_path, df_tasks)
        time.sleep(1)
