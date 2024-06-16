import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType


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


# Process the df to get buy count per minute
def users_view_product_minute(df):
    return df.groupBy(F.window('datetime', '1 minute'), 'extra_2') \
        .agg(F.countDistinct('content').alias('unique_users')) \
        .orderBy('window')


# Get ranking of the most viewed products per hour
def get_view_ranking_hour(df):
    return df.groupBy(F.window('datetime', '1 hour'), 'extra_2') \
        .count() \
        .orderBy('window', F.col('count').desc())


spark = create_spark_session()
df = read_files(spark)
df_user_view = df.filter(df["type"] == "User").filter(df["extra_1"] == "ZOOM")
df_user_view = df_user_view.withColumn('extra_2', F.regexp_replace('extra_2', 'VIEW_PRODUCT ', ''))

df_task3 = users_view_product_minute(df_user_view)
df_task3.show()
# Export the result to a csv file
df_task3.toPandas().to_csv('task3.csv', index=False)

df_task4 = get_view_ranking_hour(df_user_view)
df_task4.show()
# Export the result to a csv file
df_task4.toPandas().to_csv('task4.csv', index=False)
