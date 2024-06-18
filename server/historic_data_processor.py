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
                .config("spark.executor.extraClassPath", "postgresql-42.7.3.jar") \
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
                .getOrCreate()
            break
        except Exception as e:
            print("Not able to create the spark session: ", e)
            time.sleep(5)

    return spark


# get all files in mock\mock_files\log folder
def get_log_files(log_path):
    log_files = []
    for file in os.listdir(log_path):
        if file.endswith('.txt'):
            log_files.append(file)
    return log_files


#  Read all files and put them in the same spark dataframe
def read_files(spark, log_path):
    log_files = get_log_files(log_path)
    df = spark.read.option("delimiter", ";").option("header", True).csv(log_path + log_files[0])
    for file in log_files[1:]:
        df = df.union(spark.read.option("delimiter", ";").option("header", True).csv(log_path + file))
    print("="*5, f"The dataframe of {len(log_files)} files has been created", "="*5)

    # create a new column with the datetime
    df = df.withColumn('datetime', (F.col('timestamp') / 1e9).cast(TimestampType()))

    return df


# Process the df to get count of products bought per minute
def products_bought_minute(df_user_buy):
    return df_user_buy.groupBy(F.window('datetime', '1 minute')) \
        .count() \
        .orderBy('window')


# Get the amount earned per minute
def amount_earned_minute(df_user_view, product_df):
    # Join the df with the product_df to get the price of the product
    df_user_view = df_user_view.join(product_df, df_user_view["extra_2"] == product_df["id"], "left")

    # Calculate the amount earned
    return df_user_view.groupBy(F.window('datetime', '1 minute')) \
        .agg(F.sum('price').alias('amount_earned')) \
        .orderBy('window')
        


# Process the df to get the number of unique users that viewed a product per minute
def users_view_product_minute(df_user_view, product_df):
    # Join the df with the product_df to get the name of the product
    df_user_view = df_user_view.join(product_df, df_user_view["extra_2"] == product_df["id"], "left")

    return df_user_view.groupBy(F.window('datetime', '1 minute'), 'name') \
        .agg(F.countDistinct('content').alias('unique_users')) \
        .orderBy('window')


# Get ranking of the most viewed products per hour
def get_view_ranking_hour(df_user_view, product_df):
    # Join the df with the product_df to get the name of the product
    df_user_view = df_user_view.join(product_df, df_user_view["extra_2"] == product_df["id"], "left")

    return df_user_view.groupBy(F.window('datetime', '1 hour'), 'name') \
        .count() \
        .orderBy('window', F.col('count').desc())


# Get the median of number of views of a product before it was bought
def median_views_before_buy(df_user_view, df_user_buy):
    # Calculate number of views of a product per user
    df_user_view = df_user_view.groupBy('content', 'extra_2') \
        .agg(F.count('content').alias('views'))
    
    # For each product bought, get the number of views before it was bought for each user
    df_user_buy = df_user_buy.join(df_user_view, ['content', 'extra_2'], 'left')

    # Save the amount of buys that have the same number of views
    return df_user_buy.groupBy('views') \
        .count() \
        .orderBy('count')


# Read the log files
log_path = './mock_files/log/'
spark_local = create_spark_session_local()
df = read_files(spark_local, log_path)
df.show()


jdbc_url = f"jdbc:postgresql://{POSTGREE_CREDENTIALS['host']}:{POSTGREE_CREDENTIALS['port']}/{POSTGREE_CREDENTIALS['dbname']}"

# Create a spark session to read the postgres database
spark_post = create_spark_session_postgres()

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
        product_df.show()
        break
    except Exception as e:
        print("The products table isn't available yet: ", e)
        raise e
        time.sleep(5)

print("="*5, "Processing the data", "="*5)

df_user_buy = df.filter(df["type"] == "Audit").filter(df["extra_1"] == "BUY")

df_user_view = df.filter(df["type"] == "User").filter(df["extra_1"] == "ZOOM")
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', ''))
# Remove the final '.' from the extra_2 column
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', '\.$', ''))


df_task1 = products_bought_minute(df_user_buy)
df_task1.show()
# Export the result to a csv file not using pandas
df_task1 = df_task1.withColumn('window', F.col('window').cast('string'))
df_task1.write.csv('./tasks/task1.csv')

df_task2 = amount_earned_minute(df_user_buy, product_df)
df_task2.show()
# Export the result to a csv file
df_task2 = df_task2.withColumn('window', F.col('window').cast('string'))
df_task2.write.csv('./tasks/task2.csv')


df_task3 = users_view_product_minute(df_user_view, product_df)
df_task3.show()
# Export the result to a csv file
df_task3 = df_task3.withColumn('window', F.col('window').cast('string'))
df_task3.write.csv('./tasks/task3.csv')


df_task4 = get_view_ranking_hour(df_user_view, product_df)
df_task4.show()
# Export the result to a csv file
df_task4 = df_task4.withColumn('window', F.col('window').cast('string'))
df_task4.write.csv('./tasks/task4.csv')


df_task5 = median_views_before_buy(df_user_view, df_user_buy)
df_task5.show()
# Export the result to a csv file
df_task5.write.csv('./tasks/task5.csv')
# PRECISA CALCULAR A MEDIANA DESSES DADOS

# Print directory of the files
print("Files created in the tasks folder:")
print(os.listdir('./tasks/'))