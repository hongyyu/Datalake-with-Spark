import configparser
from datetime import datetime
import json
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


# Read the configure file
config = configparser.ConfigParser()
config.read('dl.cfg')

# Set AWS ID and KEY in the environment
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def timer(start_time=None):
    """Counting processing time
    :parameter start_time: if None start to computing time, if not None compute total processing time
    :type start_time: None, datetime
    :return start datetime or print out total processing time
    """
    # Get starting time
    if not start_time:
        start_time = datetime.now()
        return start_time
    # Calculate running time
    elif start_time:
        sec = (datetime.now() - start_time).total_seconds() % (3600 * 60)
        print('Duration: %ss' % round(sec, 2))
        
        
def create_spark_session():
    """Create spark session and spark context for processing data from S3
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def process_song_data(sc, input_data, output_data):
    """From song dataset on S3, run ETL pipeline to generate songs table
    and artists table
    :param sc: saprkContext used for loading data
    :param input_data: S3 bucket source
    :param output_data: personal S3 path to store processed tables
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*'
    
    # read song data file
    song_rdd = sc.textFile(song_data).map(lambda x: json.loads(x))
    df = song_rdd.map(lambda items: Row(**items)).toDF()
    
    # Compute time to process and store songs table
    print('Strat to process songs table...')
    start_songs = timer()
    
    # extract columns to create songs table
    # songs (song_id, title, artist_id, year, duration)
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')\
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite")\
        .partitionBy('year', 'artist_id')\
        .parquet(os.path.join(output_data, 'songs/songs.parque'))
    timer(start_songs)
    print('Done with songs table!')
    
    # Compute time to process and store artists table
    print('Strat to process artists table...')
    start_artists = timer()
    
    # extract columns to create artists table
    # artists(artist_id, name, location, latitude, longitude)
    artists_table = df.select('artist_id', 
                              col('artist_name').alias('name'), 
                              col('artist_location').alias('location'), 
                              col('artist_latitude').alias('latitude'), 
                              col('artist_longitude').alias('longitude')).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
        .parquet(os.path.join(output_data, 'artists/artists.parque'))
    timer(start_artists)
    print('Done with artists table!')


def process_log_data(sc, input_data, output_data):
    """From log and song dataset, run ETL pipeline for generate users, time, and songplays tables
    :param sc: saprkContext used for loading data
    :param input_data: S3 bucket source
    :param output_data: personal S3 path to store processed tables
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*'

    # read log data file
    log_rdd = sc.textFile(log_data).map(lambda x: json.loads(x))
    df = log_rdd.map(lambda items: Row(**items)).toDF()

    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # Compute time to process and store users table
    print('Strat to process artists table...')
    start_users = timer()
    
    # extract columns for users table
    # users (user_id, first_name, last_name, gender, level)
    users_table = df.select(col('userId').alias('user_id'), 
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level').dropDuplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite")\
        .parquet(os.path.join(output_data, 'users/users.parque'))
    timer(start_users)
    print('Done with users table!')

    # Compute time to process and store time table
    print('Strat to process time table...')
    start_time = timer()
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(ts)//1000))
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda timestamp: str(datetime.fromtimestamp(int(timestamp))))
    df = df.withColumn('datetime', get_datetime('timestamp'))

    # extract columns to create time table
    # time (start_time, hour, day, week, month, year, weekday)
    time_table = df.select(col('ts').alias('start_time'), 'datetime')\
        .withColumn('hour', hour('datetime'))\
        .withColumn('day', dayofmonth('datetime'))\
        .withColumn('week', weekofyear('datetime'))\
        .withColumn('month', month('datetime'))\
        .withColumn('year', year('datetime'))\
        .withColumn('weekday', dayofweek('datetime'))\
        .drop('datetime')\
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite")\
        .partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'time/time.parque'))
    timer(start_time)
    print('Done with time table!')

    # read in song data to use for songplays table
    song_rdd = sc.textFile(input_data + 'song_data/*/*/*').map(lambda x: json.loads(x))
    song_df = song_rdd.map(lambda items: Row(**items)).toDF()
    
    # Compute time to process and store songplays table
    print('Strat to process songplay table...')
    start_songplays = timer()
    
    # First, join two dataframes with corresponding column (artist name)
    join_df = df.join(song_df, col('artist') == col('artist_name'))
    
    # extract columns from joined song and log datasets to create songplays table
    # songplays (songplay_id, start_time, user_id, level, song_id, artist_id, 
    # session_id, location, user_agent)
    songplays_table = join_df.withColumn("songplay_id", monotonically_increasing_id())\
        .select('songplay_id',
                col('datetime').alias('start_time'),
                col('userId').alias('user_id'),
                col('level').alias('level'),
                col('song_id').alias('song_id'),
                col('artist_id').alias('artist_id'),
                col('sessionId').alias('session_id'),
                col('location').alias('location'),
                col('userAgent').alias('user_agent'),
                year('datetime').alias('year'),
                month('datetime').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite")\
        .partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'songplays/songplays.parque'))
    timer(start_songplays)
    print('Done with songplays table!')


def main():
    """ETL pipeline to buile data lake on AWS
    1. Create sparkContext for loading data
    2. Process song data
    3. Process log data
    """
    spark, sc = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(sc, input_data, output_data)
    process_log_data(sc, input_data, output_data)


if __name__ == "__main__":
    main()
