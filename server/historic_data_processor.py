import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window


# Create a spark session
def create_spark_session():
    spark = SparkSession.builder.appName('mock').getOrCreate()
    return spark


# get all files in mock\mock_files\log folder
def get_log_files():
    log_files = []
    for file in os.listdir('../mock/mock_files/log'):
        if file.endswith('.txt'):
            log_files.append(file)
    return log_files


#  Read all files and put them in the same spark dataframe
def read_files(spark):
    log_files = get_log_files()
    df = spark.read.option("delimiter", ";").option("header", True).csv('../mock/mock_files/log/' + log_files[0])
    for file in log_files[1:]:
        df = df.union(spark.read.option("delimiter", ";").option("header", True).csv('../mock/mock_files/log/' + file))
    print("Read all files")
    
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



spark = create_spark_session()

# Read the products file
product_df = spark.read.option("delimiter", ";").option("header", True).csv('../mock/mock_files/csv/products.csv')
df = read_files(spark)


df_user_buy = df.filter(df["type"] == "Audit").filter(df["extra_1"] == "BUY")

df_user_view = df.filter(df["type"] == "User").filter(df["extra_1"] == "ZOOM")
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', ''))
# Remove the final '.' from the extra_2 column
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', '\.$', ''))


df_task1 = products_bought_minute(df_user_buy)
df_task1.show()
# Export the result to a csv file
df_task1.toPandas().to_csv('task1.csv', index=False)


df_task2 = amount_earned_minute(df_user_buy, product_df)
df_task2.show()
# Export the result to a csv file
df_task2.toPandas().to_csv('task2.csv', index=False)


df_task3 = users_view_product_minute(df_user_view, product_df)
df_task3.show()
# Export the result to a csv file
df_task3.toPandas().to_csv('task3.csv', index=False)


df_task4 = get_view_ranking_hour(df_user_view, product_df)
df_task4.show()
# Export the result to a csv file
df_task4.toPandas().to_csv('task4.csv', index=False)


df_task5 = median_views_before_buy(df_user_view, df_user_buy)
df_task5.show()
# Export the result to a csv file
df_task5.toPandas().to_csv('task5.csv', index=False)
# PRECISA CALCULAR A MEDIANA DESSES DADOS
