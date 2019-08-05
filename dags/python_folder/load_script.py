import datetime
import os
import pandas as pd
import pyspark.sql
from pyspark.sql import SparkSession as SS
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


def create_spark_session():
    spark = SS \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

spark = create_spark_session()

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"/song/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table_stage = df['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name',
                           'duration', 'num_songs', 'song_id', 'title', 'year']

    songs_table_stage.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.par'), 'overwrite')
    songs_table_stage.show()
    #find a way to move this to properties folder and import the connection
    #below supported by dbt but cannot select schema, only reads to public via spark df + sqlalchemy is an option but not ideal to fet more imports
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/postgres"
    properties =
    songs_table_stage.write.jdbc(url=url,table="new", mode=mode, properties=properties)

    # So below might be the ideal option to use but doesnt interact with dbt
    # songs_table_stage.write\
    # .format("jdbc")\
    # .option("url", "jdbc:mysql://127.0.0.1:3306/STAGING?useSSL=false")\
    # .option("dbtable", "SONG_STAGING").option("user", "root")\
    # .option("password", "").mode('ignore').save()


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+"/log/*.json"
    df = spark.read.json(log_data)

    # extract columns for users table
    log_table_stage = df['userId','firstName','lastName','gender','level']
    #drop dupes
    log_table_stage = log_table_stage.dropDuplicates(['userId'])
    log_table_stage.show()
    # write users table to parquet files
    log_table_stage.write.parquet(os.path.join(output_data, 'log.par'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data =  "/Users/mahdimostafa/ELT-Project-Apollo/dags/python_folder/input_data"
    output_data = "/Users/mahdimostafa/ELT-Project-Apollo/dags/python_folder/output_data"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
