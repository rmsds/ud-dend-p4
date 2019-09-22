import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions \
        import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    if input_data.startswith('s3a://'):
        # we are reading data from S3
        song_data = "{}{}song_data".format(
                '/' if input_data.endswith('/') else '',
                input_data
        )
    else:
        # we are reading local files
        import glob
        glob_pattern = "{}/song_data/*/*/*/*.json".format(input_data)
        song_data = glob.glob(glob_pattern) 
        if 0 == len(log_data):
            print("[ERROR] could not find any log data files:'{}'".format(glob_pattern))
            exit(0)

    # read song data file
    ##import pyspark.sql.types as pstypes
    ##songSchema = pstypes.StructType([
    ##    pstypes.StructField("num_songs",        pstypes.IntegerType()),
    ##    pstypes.StructField("artist_id",        pstypes.StringType()),
    ##    pstypes.StructField("artist_latitude",  pstypes.FloatType()),
    ##    pstypes.StructField("artist_longitude", pstypes.FloatType()),
    ##    pstypes.StructField("artist_location",  pstypes.StringType()),
    ##    pstypes.StructField("artist_name",      pstypes.StringType()),
    ##    pstypes.StructField("song_id",          pstypes.StringType()),
    ##    pstypes.StructField("title",            pstypes.StringType()),
    ##    pstypes.StructField("duration",         pstypes.FloatType()),
    ##    pstypes.StructField("year",             pstypes.IntegerType()),
    ##])
    ##df = spark.read.json(*song_data, schema=songSchema)
    df = spark.read.json(song_data)
    print('DF.COUNT', df.count())
    ##df.printSchema()
    ##df.show(5)

    # extract columns to create songs table
    songs_table = df.select(
            'song_id',
            'title',
            'artist_id',
            'year',
            'duration'
    ).dropDuplicates()
    print('SONGS.COUNT', songs_table.count())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
            "{}/songs".format(output_data),
            partitionBy=['year', 'artist_id'],
            mode='overwrite'
    )

    # extract columns to create artists table
    artists_table = df.select(
            'artist_id',
            df.artist_name.alias('name'),
            df.artist_location.alias('location'),
            df.artist_latitude.alias('latitude'),
            df.artist_longitude.alias('longitude')
    ).dropDuplicates()
    print('ARTISTS.COUNT', artists_table.count())

    # write artists table to parquet files
    artists_table.write.parquet(
            "{}/artists".format(output_data),
            mode='overwrite'
    )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    if input_data.startswith('s3a://'):
        # we are reading data from S3
        log_data = "{}{}log_data".format(
                '/' if input_data.endswith('/') else '',
                input_data
        )
    else:
        # we are reading local files
        import glob
        glob_pattern = "{}/log_data/*/*/*.json".format(input_data)
        log_data = glob.glob(glob_pattern)
        if 0 == len(log_data):
            print("[ERROR] could not find any log data files:'{}'".format(
                glob_pattern
            ))
            exit(0)

    # read log data file
    df = spark.read.json(log_data)
    print('DF.COUNT', df.count())

    return
    # filter by actions for song plays
    df = ''

    # extract columns for users table
    artists_table = ''

    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = ''

    # create datetime column from original timestamp column
    get_datetime = udf()
    df = ''

    # extract columns to create time table
    time_table = ''

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = ''

    # extract columns from joined song and log datasets to create songplays
    # table
    songplays_table = ''

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    input_data = 'data/'
    output_data = "output-data/"

    # process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
