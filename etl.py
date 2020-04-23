import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf, col, to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This procedure creates a spart session
    It adds the aws hadoop package that would provide utilities for usage on s3 bucket
    

    NO INPUTS:
    '''        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This procedure processes song data infomation with spark into dimensions tables that are saved to parquet files.

    INPUTS:
    * spark object variable
    * input data s3 path variable pointing to the s3 bucket source of data
    * output data s3 path variable pointing to the s3 bucket to save the table files of data
    '''       
    # get filepath to song data file
    sparkify_log_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    song_data = spark.read.json(sparkify_log_data)

    # extract columns for users table
    songs_df = song_data.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    songs_df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs = spark.sql("""
        SELECT DISTINCT(song_id), title, artist_id, year, duration
        FROM songs
        WHERE song_id is NOT NULL
        """)

    # write songs table to parquet files partitioned by year and artist
    songs.write.partitionBy("year","artist_id").parquet(output_data+"sparkify/songs/songs.parquet",mode="overwrite")

    # extract columns for artist table
    artist_df = song_data.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    artist_df.createOrReplaceTempView("artists")

    # extract columns to create artists table
    artists = spark.sql("""
        SELECT 
            DISTINCT(artist_id), 
            artist_name AS name, 
            artist_location AS location, 
            artist_latitude AS latitude, 
            artist_longitude AS longitude
        FROM artists
        WHERE artist_id is NOT NULL
        """)

    # write artists table to parquet files
    artists.write.parquet(output_data+"sparkify/artists/artists.parquet",mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    This procedure processes log data infomation with spark into fact tables that are saved to parquet files.

    INPUTS:
    * spark object variable
    * input data s3 path variable pointing to the s3 bucket source of data
    * output data s3 path variable pointing to the s3 bucket to save the table files of data
    '''           
    # read log data file
    sparkify_log_data = input_data + 'log_data/*/*/*.json'

    log_data = spark.read.json(sparkify_log_data)

    # filter by actions for song plays
    log_data_df = log_data.filter(log_data['page']=='NextSong')

    # extract columns for users table
    users_df = log_data_df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    users_df.createOrReplaceTempView("users")
    users = spark.sql("""
        SELECT DISTINCT(userId) as user_id, firstName as first_name, lastName as last_name, gender, level
        FROM users
        WHERE userId is NOT NULL
        """)

    # write users table to parquet files
    users.write.parquet(output_data+"sparkify/users/users.parquet",mode="overwrite")



    # create timestamp column from original timestamp column
    log_data_df = log_data_df.withColumn("ts", (log_data_df["ts"]/1000.0).cast(IntegerType()))
    # create datetime column from original timestamp column
    log_data_df = log_data_df.withColumn("timestamp", to_timestamp(log_data_df["ts"]).cast(DateType()))

    time_df = log_data_df.select(['ts','timestamp'])

    time_df.createOrReplaceTempView("time")

    # extract columns to create time table
    time = spark.sql("""
        SELECT
            DISTINCT(ts) as start_time,
            EXTRACT(hour FROM timestamp) AS hour, 
            EXTRACT(day  FROM timestamp) AS day,
            EXTRACT(week  FROM timestamp) AS week, 
            EXTRACT(month  FROM timestamp) AS month, 
            EXTRACT(year FROM timestamp) AS year,
            EXTRACT(dayofweek  FROM timestamp) AS weekday         
        FROM time
        WHERE ts is NOT NULL
        """)

    # write time table to parquet files partitioned by year and month
    time.write.partitionBy("year","month").parquet(output_data+"sparkify/time/time.parquet",mode="overwrite")

    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"sparkify/songs/songs.parquet")
    
    song_df.createOrReplaceTempView("song")
    log_data_df.createOrReplaceTempView("events")
    # extract columns from joined song and log datasets to create songplays table 

    songplays = spark.sql("""
        SELECT
                ts AS start_time, 
                e.userId AS user_id, 
                e.level AS level, 
                s.song_id AS song_id, 
                s.artist_id AS artist_id,
                e.sessionId AS session_id, 
                e.location AS location, 
                e.userAgent AS user_agent
        FROM song s
        JOIN events e 
        on (s.artist_name=e.artist)
        WHERE start_time is NOT NULL 
        and user_id is NOT NULL 
        and song_id is NOT NULL 
        and artist_id is NOT NULL 
        and session_id is NOT NULL
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays.write.partitionBy("year","month").parquet(output_data+"sparkify/songplays/songplays.parquet",mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://arita-bucket-demo/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
