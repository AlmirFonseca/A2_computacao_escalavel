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


# get all files in mock\mock_files\log folder removing the already read files
def get_log_files(log_path, already_read_files=[]):
    log_files = []

    # Get all files inside the folders in the log_path
    for root, _, files in os.walk(log_path):
        files = [root + "/" + file for file in files if (root + "/" + file) not in already_read_files]
        log_files.extend(files)
    
    return log_files


#  Read all files and put them in the same spark dataframe
def read_files(spark, log_files):
    df = spark.read.option("delimiter", ";").option("header", True).csv(log_files[0])
    for file in log_files[1:]:
        df = df.union(spark.read.option("delimiter", ";").option("header", True).csv(file))
    print("="*5, f"The dataframe of {len(log_files)} files has been created", "="*5)

    # create a new column with the datetime
    df = df.withColumn('datetime', (F.col('timestamp') / 1e9).cast(TimestampType()))

    return df, log_files


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


# Function to process all the data
def process_data(redis_client, spark_local, spark_post, log_files, df_tasks={}):
    df, files_already_read = read_files(spark_local, log_files)

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
            break
        except Exception as e:
            print("The products table isn't available yet: ", e)
            time.sleep(5)


    print("="*5, "Processing the data", "="*5)

    # Filter the data to get only the buys and the views
    df_user_buy = df.filter(df["type"] == "Audit").filter(df["extra_1"] == "BUY") # Get only the buys

    df_user_view = df.filter(df["type"] == "User").filter(df["extra_1"] == "ZOOM") # Get only the views of the products
    df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', '')) # Remove the 'VIEW_PRODUCT ' from the extra_2 column
    df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', '\.$', '')) # Remove the final '.' from the extra_2 column


    # ---------- Task 1 ----------
    print("="*5, "Calculando o número de produtos comprados por minuto", "="*5)
    df_task1 = products_bought_minute(df_user_buy)
    df_task1 = df_task1.withColumn('window_start', F.col('window')['start'].cast('string')) \
        .withColumn('window_end', F.col('window')['end']) \
        .drop('window')
    
    # If there is already data in the df_tasks, merge the dataframes
    if df_tasks.get('purchases_per_minute'):
        # Merge the dataframes, summing grouped by window_start, window_end, store_id and count
        df_task1 = df_task1.unionByName(df_tasks['purchases_per_minute']) \
            .groupBy('window_start', 'window_end', 'store_id') \
            .agg(F.sum('count').alias('count')) \
            .orderBy('window_end', 'store_id')        

    print("="*5, "ENVIANDO TAREFA 1 PARA O REDIS", "="*5)
    # Get the last minute of the dataframe for each store
    df_task1_last_data = get_df_last_minute(df_task1, 'window_end', 'store_id', ['count', 'window_start', 'window_end'])
    send_to_redis_as_dict(redis_client, df_task1_last_data, 'purchases_per_minute')


    # ---------- Task 2 ----------
    print("="*5, "Calculando o valor faturado por minuto", "="*5)
    df_task2 = amount_earned_minute(df_user_buy, product_df)
    df_task2 = df_task2.withColumn('window_start', F.col('window')['start'].cast('string')) \
        .withColumn('window_end', F.col('window')['end']) \
        .drop('window')

    # If there is already data in the df_tasks, merge the dataframes
    if df_tasks.get('revenue_per_minute'):
        # Merge the dataframes, summing grouped by window_start, window_end, store_id and amount_earned
        df_task2 = df_task2.unionByName(df_tasks['revenue_per_minute']) \
            .groupBy('window_start', 'window_end', 'store_id') \
            .agg(F.sum('amount_earned').alias('amount_earned')) \
            .orderBy('window_end', 'store_id')

    print("="*5, "ENVIANDO A TAREFA 2 PARA O REDIS", "="*5)
    # Obtain the last minute of the dataframe for each store
    df_task2_last_data = get_df_last_minute(df_task2, 'window_end', 'store_id', ['amount_earned', 'window_start', 'window_end'])
    send_to_redis_as_dict(redis_client, df_task2_last_data, 'revenue_per_minute')


    # ---------- Task 3 ----------
    print("="*5, "Calculando o número de usuários únicos visualizando cada produto por minuto", "="*5)
    df_task3 = users_view_product_minute(df_user_view, product_df)
    df_task3 = df_task3.withColumn('window_start', F.col('window')['start'].cast('string')) \
        .withColumn('window_end', F.col('window')['end']) \
        .drop('window')

    # If there is already data in the df_tasks, merge the dataframes
    if df_tasks.get('unique_users_per_minute'):
        # Merge the dataframes, summing grouped by window_start, window_end, name and store_id
        df_task3 = df_task3.unionByName(df_tasks['unique_users_per_minute']) \
            .groupBy('window_start', 'window_end', 'name', 'store_id') \
            .agg(F.sum('unique_users').alias('unique_users')) \
            .orderBy('window_end', 'store_id', F.desc('unique_users'))

    print("="*5, "ENVIANDO A TAREFA 3 PARA O REDIS", "="*5)
    df_task3_last_data = get_df_last_minute(df_task3, 'window_end', 'store_id', ['name', 'unique_users', 'window_start', 'window_end'])
    send_to_redis_as_dict(redis_client, df_task3_last_data, 'unique_users_per_minute')


    # ---------- Task 4 ----------
    print("="*5, "Calculando o número de visualizações de cada produto por hora", "="*5)
    df_task4 = get_view_ranking_hour(df_user_view, product_df)
    df_task4 = df_task4.withColumn('window_start', F.col('window')['start'].cast('string')) \
        .withColumn('window_end', F.col('window')['end']) \
        .drop('window')

    # If there is already data in the df_tasks, merge the dataframes
    if df_tasks.get('ranking_viewed_products_per_hour'):
        # Merge the dataframes, summing grouped by window_start, window_end, name and store_id
        df_task4 = df_task4.unionByName(df_tasks['ranking_viewed_products_per_hour']) \
            .groupBy('window_start', 'window_end', 'name', 'store_id') \
            .agg(F.sum('views').alias('views')) \
            .orderBy('window_end', 'store_id', F.desc('views'))

    print("="*5, "ENVIANDO A TAREFA 4 PARA O REDIS", "="*5)
    df_task4_last_data = get_df_last_minute(df_task4, 'window_end', 'store_id', ['name', 'views', 'window_start', 'window_end'])
    send_to_redis_as_dict(redis_client, df_task4_last_data, 'ranking_viewed_products_per_hour')


    # ---------- Task 5 ----------
    # PRECISA CALCULAR A MEDIANA DESSES DADOS
    print("="*5, "Calculando a mediana do número de vezes que um usuário visualiza um produto antes de efetuar uma compra", "="*5)
    df_task5 = median_views_before_buy(df_user_view, df_user_buy)

    # If there is already data in the df_tasks, merge the dataframes
    if df_tasks.get('median_views_before_buy'):
        # Merge the dataframes, summing grouped by views and store_id
        df_task5 = df_task5.unionByName(df_tasks['median_views_before_buy']) \
            .groupBy('views', 'store_id') \
            .agg(F.sum('count').alias('count')) \
            .orderBy('store_id', 'views')

    print("="*5, "ENVIANDO A TAREFA 5 PARA O REDIS", "="*5)
    df_task5_grouped = get_df_grouped(df_task5, 'store_id', ['views', 'count'])
    send_to_redis_as_dict(redis_client, df_task5_grouped, 'median_views_before_buy')


    # Save the dataframes in the df_tasks dictionary
    df_tasks = {
        'purchases_per_minute': df_task1,
        'revenue_per_minute': df_task2,
        'unique_users_per_minute': df_task3,
        'ranking_viewed_products_per_hour': df_task4,
        'median_views_before_buy': df_task5
    }

    print("="*5, "Data processing finished", "="*5)

    return files_already_read, df_tasks


# Create a spark session to read the postgres database
spark_post = create_spark_session_postgres()
spark_local = create_spark_session_local()

# Create a redis connection and flush the database
redis_client = redis.Redis(host=os.environ.get('REDIS_HOST'), port=os.environ.get('REDIS_PORT'))
redis_client.flushall()

# Define the path of the log files
log_path = './mock_files/logs/'


# Initial read of the files
initial_files = get_log_files(log_path)
files_read, df_tasks = process_data(redis_client, spark_local, spark_post, initial_files)


# Incremental read of the files 
while True:
    new_files = get_log_files(log_path, files_read)
    if len(new_files) == 0:
        print("="*5, "No new files to process", "="*5)
    else:
        new_files_read, df_tasks = process_data(redis_client, spark_local, spark_post, new_files, df_tasks)
        files_read.extend(new_files_read)
        files_read += new_files
    # Wait 5 seconds before checking for new files
    time.sleep(5)