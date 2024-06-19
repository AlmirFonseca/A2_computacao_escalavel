import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
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

    # Convert the store_id column to integer
    df = df.withColumn('store_id', F.col('store_id').cast('integer'))

    return df


# Process the df to get count of products bought per minute
def products_bought_minute(df_user_buy):
    return df_user_buy.groupBy(F.window('datetime', '1 minute'), 'store_id') \
        .count() \
        .orderBy('window', 'store_id')


# Get the amount earned per minute
def amount_earned_minute(df_user_view, product_df):
    # Join the df with the product_df to get the price of the product 
    df_user_view = df_user_view.alias('df_user_view')
    df_user_view = df_user_view.join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")

    # Calculate the amount earned
    return df_user_view.groupBy(F.window('datetime', '1 minute'), 'df_user_view.store_id') \
        .agg(F.sum('price').alias('amount_earned')) \
        .orderBy('window', 'store_id')
        


# Process the df to get the number of unique users that viewed a product per minute
def users_view_product_minute(df_user_view, product_df):
    # Join the df with the product_df to get the name of the product
    df_user_view = df_user_view.alias('df_user_view')
    df_user_view = df_user_view.join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")

    df_final = df_user_view.groupBy(F.window('datetime', '1 minute'), 'name', 'df_user_view.store_id') \
        .agg(F.countDistinct('content').alias('unique_users'))
    
    # Window to get the top 10 of each group
    window = Window.partitionBy('window', 'store_id').orderBy("unique_users", ascending=False)
    
    # Group the dataframe by window, store_id and unique_users and get the top 10 of each group
    return df_final.withColumn('rank', F.dense_rank().over(window)) \
        .filter(F.col('rank') <= 10)


# Get ranking of the most viewed products per hour
def get_view_ranking_hour(df_user_view, product_df):
    # Join the df with the product_df to get the name of the product
    df_user_view = df_user_view.alias('df_user_view')
    df_user_view = df_user_view.join(product_df, (df_user_view["extra_2"] == product_df["id"]) & (df_user_view["store_id"] == product_df["store_id"]), "left")

    df_final = df_user_view.groupBy(F.window('datetime', '1 hour'), 'name', 'df_user_view.store_id') \
        .agg(F.count('content').alias('views'))
    
    # Window to get the top 10 of each group
    window = Window.partitionBy('window', 'store_id').orderBy("views", ascending=False)

    # Group the dataframe by window, store_id and views and get the top 10 of each group
    return df_final.withColumn('rank', F.dense_rank().over(window)) \
        .filter(F.col('rank') <= 10)


# Get the median of number of views of a product before it was bought
def median_views_before_buy(df_user_view, df_user_buy):
    # Calculate number of views of a product per user
    df_user_view = df_user_view.groupBy('content', 'extra_2', 'store_id') \
        .agg(F.count('content').alias('views'))
    
    # Fill the "" values with 0
    df_user_view = df_user_view.na.fill({'views': 0})
    
    # For each product bought, get the number of views before it was bought for each user
    df_user_buy = df_user_buy.alias('df_user_buy')
    df_user_buy = df_user_buy.join(df_user_view, ['content', 'extra_2', 'store_id'], 'left')

    # Save the amount of buys that have the same number of views
    df_final = df_user_buy.groupBy('views', 'df_user_buy.store_id') \
        .count() \
        .orderBy('df_user_buy.store_id', 'views')

    return df_final

# Create a spark session to read the postgres database
spark_post = create_spark_session_postgres()

# Read the log files
log_path = './mock_files/requests/'
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

df_user_buy = df.filter(df["type"] == "Audit").filter(df["extra_1"] == "BUY") # Get only the buys

df_user_view = df.filter(df["type"] == "User").filter(df["extra_1"] == "ZOOM") # Get only the views of the products
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', '')) # Remove the 'VIEW_PRODUCT ' from the extra_2 column
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', '\.$', '')) # Remove the final '.' from the extra_2 column


df_task1 = products_bought_minute(df_user_buy)
print("="*5, "Número de produtos comprados por minuto:")
df_task1.show()
df_task1 = df_task1.withColumn('window', F.col('window').cast('string')) # convert the window column to string
df_task1.coalesce(1).write.option("header", "true").csv('./tasks/task1/', mode='overwrite')


df_task2 = amount_earned_minute(df_user_buy, product_df)
print("="*5, "Valor faturado por minuto:")
df_task2.show()
df_task2 = df_task2.withColumn('window', F.col('window').cast('string')) # convert the window column to string
df_task2.coalesce(1).write.option("header", "true").csv('./tasks/task2/', mode='overwrite')


df_task3 = users_view_product_minute(df_user_view, product_df)
print("="*5, "Número de usuários únicos visualizando cada produto por minuto:")
df_task3.show()
df_task3 = df_task3.withColumn('window', F.col('window').cast('string')) # convert the window column to string
df_task3.coalesce(1).write.option("header", "true").csv('./tasks/task3/', mode='overwrite')


df_task4 = get_view_ranking_hour(df_user_view, product_df)
print("="*5, "Ranking dos produtos mais visualizados por hora:")
df_task4.show()
df_task4 = df_task4.withColumn('window', F.col('window').cast('string')) # convert the window column to string
df_task4.coalesce(1).write.option("header", "true").csv('./tasks/task4/', mode='overwrite')


df_task5 = median_views_before_buy(df_user_view, df_user_buy)
print("="*5, "Mediana do número de vezes que um usuário visualiza um produto antes de efetuar uma compra:")
df_task5.show()
df_task5.coalesce(1).write.option("header", "true").csv('./tasks/task5/', mode='overwrite')
# PRECISA CALCULAR A MEDIANA DESSES DADOS
