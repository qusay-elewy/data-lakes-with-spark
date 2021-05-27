from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
import calendar
import os
import configparser


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Loops through the song files in the specified path and extracts the targeted fields necessary for building our star schema.
    """

    # get filepath to song data file
    #song_data = "s3://udacity-dend/song_data"
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"

    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    #songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).collect()
    songs_table = df["song_id", "title", "artist_id", "year", "duration"]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.partitionBy("year","artist").parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("songs.parquet saved.")

    # extract columns to create artists table
    #artists_table = df.select(["artist_id", "name", "location", "lattitude", "longitude"]).collect()
    artists_table = df["artist_id", "name", "location", "lattitude", "longitude"]
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("artists.parquet saved.")


def process_log_data(spark, input_data, output_data):
    """Loops through the log files in the specified path and extracts the targeted fields necessary for building our star schema.
    """
    
    
    # get filepath to log data file
    #log_data = "s3://udacity-dend/log_data"
    log_data = "s3a://udacity-dend/log-data/*.json"


    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    #df = df.filter(df["page"] == "NextPage").collect()
    df = df.filter(df["page"] == "NextPage")
    
    # extract columns for users table    
    #users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).collect()
    users_table = df["userId", "firstName", "lastName", "gender", "level"]
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users.parquet"), 'overwrite')
    print("users.parquet saved.")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x) // 1000.0))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x // 1000.0))
    
    # extract columns to create time table
    # Build these fields
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_month = udf(lambda x: x.month)
    get_year = udf(lambda x: x.year)
    get_week = udf(lambda x: calendar.day_name[x.weekday()])
    get_weekday = udf(lambda x: x.isocalendar()[1])
    
    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('hour', get_hour(df.start_time))
    df = df.withColumn('day', get_day(df.start_time))
    df = df.withColumn('week', get_week(df.start_time))
    df = df.withColumn('month', get_month(df.start_time))
    df = df.withColumn('year', get_year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))
    
    #time_table = df.select(["start_time", "hour", "day", "week", "month", "year", "weekday"]).collect()
    time_table = df["start_time", "hour", "day", "week", "month", "year", "weekday"]
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("time.parquet saved.")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    df= df.join(song_df, song_df.title == df.song)
    #songplays_table = df.select(["start_time", "userId", "level", "song_id", "artist_id", "session_id", "location", "userAgent"]).collect()
    songplays_table = df["start_time", "userId", "level", "song_id", "artist_id", "session_id", "location", "userAgent"]
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("songplays.parquet saved.")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
