#!/usr/bin/env python3
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
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


def list_matching_in_bucket(
        bucket_url,
        prefix,
        suffix='.json',
        region='us-west-2'
):
    ''' Lists files in a bucket matching a prefix/suffix

    Given a bucket url and prefix will return a list of all files
    matching under that prefix and matching an optional suffix
    ('.json' by default). The region can also be specified.

    The list of results includes the prefix.
    '''
    import boto3

    if '://' in bucket_url:
        bucket_name = bucket_url.split('://')[1].split('/')[0]
    else:
        exit(0)

    client = boto3.client('s3', region_name=region)
    paginator = client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket_name,
                            'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)

    found = []
    for page in page_iterator:
        a = [item['Key']
             for item in page['Contents']
             if item['Key'].endswith(suffix)
             ]
        found.extend(a)

    found_prefix = "{}{}".format(
        bucket_url,
        '/' if not bucket_url.endswith('/') else '',
    )
    full_found = [found_prefix + i for i in found]

    return full_found


def process_song_data(spark, input_data, output_data):
    ''' Read all song data and write artists and songs tables.

    Song data is read from any json files found under `input_data`/song_data.

    Data can be read and written from/to local files or S3 buckets ('s3a://').
    Output data is written as parquet files.
    '''
    # get filepath to song data file
    if input_data.startswith('s3a://'):
        # we are reading data from S3
        song_data = list_matching_in_bucket(input_data, 'song_data/A/R')
    else:
        # we are reading local files
        import glob
        glob_pattern = "{}/song_data/*/*/*/*.json".format(input_data)
        song_data = glob.glob(glob_pattern)
        if 0 == len(song_data):
            print("[ERROR] could not find any log data files:'{}'".format(
                glob_pattern)
            )
            exit(0)

    df = spark.read.json(song_data)
    print('DF.COUNT', df.count())

    # extract columns to create songs table
    songs_table = df.select(
            'song_id',
            'title',
            'artist_id',
            'year',
            'duration'
    ).dropDuplicates()
    print("[INFO] saving information for {} songs".format(songs_table.count()))
    print("[INFO] songs_table schema:")
    songs_table.printSchema()

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
    print("[INFO] saving information for {} artists".format(
        artists_table.count())
    )
    print("[INFO] artists_table schema:")
    artists_table.printSchema()

    # write artists table to parquet files
    artists_table.write.parquet(
            "{}/artists".format(output_data),
            mode='overwrite'
    )


def process_log_data(spark, input_data, output_data):
    ''' Read log data and write users, time and songplays tables.

    Log data is read from any json files found under `input_data`/log_data.

    Data can be read and written from/to local files or S3 buckets ('s3a://').
    Output data is written as parquet files.
    '''
    # get filepath to log data file
    if input_data.startswith('s3a://'):
        # we are reading data from S3
        log_data = list_matching_in_bucket(input_data, 'log_data/')
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
    print("[INFO] read {} events".format(df.count()))
    print("[INFO] detected schema:")
    df.printSchema()
    df.show(5)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    print("[INFO] selected {} 'NextSong' events".format(df.count()))

    # extract columns for users table
    users_table = df.select(
            df.userId.alias('user_id'),
            df.firstName.alias('first_name'),
            df.lastName.alias('last_name'),
            df.gender,
            df.level
    ).dropDuplicates()
    print("[INFO] saving information for {} users".format(users_table.count()))
    print("[INFO] users_table schema:")
    users_table.printSchema()

    # write users table to parquet files
    users_table.write.parquet(
            "{}/users".format(output_data),
            mode='overwrite'
    )

    # create datetime column from original timestamp column
    import pyspark.sql.types as pstypes
    get_datetime = udf(
            lambda ts: datetime.fromtimestamp(ts/1000.0),
            pstypes.TimestampType()
    )
    df = df.withColumn('datetime', get_datetime(df.ts))
    # df.printSchema()
    # df.show(2)

    # extract columns to create time table
    time_table = df.select(
            df.datetime.alias('start_time'),
            hour(df.datetime).alias('hour'),
            dayofmonth(df.datetime).alias('day'),
            weekofyear(df.datetime).alias('week'),
            month(df.datetime).alias('month'),
            year(df.datetime).alias('year'),
            date_format(df.datetime, 'E').alias('weekday')
    ).dropDuplicates()
    print("[INFO] saving information for {} timestamps".format(
        time_table.count())
    )
    print("[INFO] time_table schema:")
    time_table.printSchema()
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
            "{}/times".format(output_data),
            partitionBy=['year', 'month'],
            mode='overwrite'
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs".format(output_data))
    artist_df = spark.read.parquet("{}/artists".format(output_data))

    # extract columns from joined song and log datasets to create songplays
    # table
    songplays_table = df.join(
            artist_df,
            artist_df.name == df.artist,
            'inner'
    ).join(
            song_df,
            [
                song_df.artist_id == artist_df.artist_id,
                song_df.title == df.song,
            ],
            'inner'
    ).select(
            monotonically_increasing_id().alias('songplay_id'),
            df.datetime.alias('start_time'),
            df.userId.alias('user_id'),
            df.level.alias('level'),
            song_df.song_id,
            artist_df.artist_id,
            df.sessionId.alias('session_id'),
            df.location.alias('location'),
            df.userAgent.alias('user_agent'),
            # needed for writing the tables partitioned
            month(df.datetime).alias('month'),
            year(df.datetime).alias('year'),
    )
    print("[INFO] saving information for {} songplays".format(
        songplays_table.count())
    )
    print('[INFO] songplays_table schema:')
    songplays_table.printSchema()
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
            "{}/songplays".format(output_data),
            partitionBy=['year', 'month'],
            mode='overwrite'
    )


def main():
    spark = create_spark_session()

    # input_data/output_data can be either on the local filesystem or
    # in S3 buckets (`s3a://`)
    # input_data = 'data/'
    input_data = 's3a://udacity-dend/'
    # output_data = 'output-data/'
    output_data = 's3a://ud-dend-proj4-rs-output/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
