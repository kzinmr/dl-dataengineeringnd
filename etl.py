import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    col,
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    date_format,
    to_timestamp,
    monotonically_increasing_id,
)


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS",
                                                 "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """Initialize a Spark session object.

    Args:
    Returns: Spark session object.
    Raises:
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Convert JSON files into `songs` and `artists` tables.
    `songs` tables are saved as parquet files partitioned by year and artist.
    `artists` tables are saved as parquet files without partition.

    Args:
        spark: Spark session object.
        input_data: The S3 path of input JSON files.
        output_data: The S3 path of output parquet files.
    Returns:
    Raises:
    """
    # get filepath to song data file
    # NOTE: restrict the part of files (song_data/A/A/A/)
    # due to long processing time
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df[songs_cols]
    songs_table = songs_table.dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, "songs.parquet")
    songs_table.write.partitionBy("year", "artist_id").parquet(
        songs_output, "overwrite"
    )

    # extract columns to create artists table
    artists_cols = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude",
    ]
    artists_table = df[artists_cols]
    artists_table = artists_table.dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_output = os.path.join(output_data, "artists.parquet")
    artists_table.write.parquet(artists_output, "overwrite")


def process_log_data(spark, input_data, output_data):
    """Convert JSON files into `users`, `time` and `songplays` tables.
    `users` tables are saved as parquet files without partition.
    `time` tables are saved as parquet files partitioned by year and month.
    `songplays` tables are saved as parquet files partitioned by year and
    month.

    Args:
        spark: Spark session object.
        input_data: The S3 path of input JSON files.
        output_data: The S3 path of output parquet files.
    Returns:
    Raises:
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    users_cols = ["userId", "firstName", "lastName", "gender", "level"]
    time_cols = ["ts"]
    songplays_cols = [
        "song",
        "ts",
        "userId",
        "level",
        "sessionId",
        "location",
        "userAgent",
    ]
    actions_cols = sorted(set(users_cols + time_cols + songplays_cols))
    df = df[actions_cols]

    # extract columns for users table
    print("Making users table...")
    users_table = df[users_cols]
    users_table = users_table.dropDuplicates(["userId"])

    # write users table to parquet files
    users_output = os.path.join(output_data, "users.parquet")
    users_table.write.parquet(users_output, "overwrite")

    # create timestamp column from original timestamp column
    print("Making time table...")
    get_timestamp = udf(lambda x: str(int(x) // 1000))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) // 1000)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    df = df.withColumn("weekday", date_format(to_timestamp(df.timestamp), "E"))
    time_table = df.select(
        col("datetime").alias("start_time"),
        hour("datetime").alias("hour"),
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"),
        month("datetime").alias("month"),
        year("datetime").alias("year"),
        col("weekday").alias("weekday"),
    )

    time_table = time_table.dropDuplicates(["start_time"])

    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, "time.parquet")
    time_table.write.partitionBy("year", "month") \
                    .parquet(time_output, "overwrite")

    # read in song data to use for songplays table
    print("Making songplays table...")
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays
    # table
    df = df.join(song_df, df.song == song_df.title)
    songplays_table = df.select(
        col("ts").alias("start_time"),
        col("userId").alias("user_id"),
        col("level").alias("level"),
        col("song_id").alias("song_id"),
        col("artist_id").alias("artist_id"),
        col("sessionId").alias("session_id"),
        col("location").alias("location"),
        col("userAgent").alias("user_agent"),
        month("datetime").alias("month"),
        year("datetime").alias("year"),
    )
    songplays_table.select(monotonically_increasing_id().alias("songplay_id")
                           ).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_output = os.path.join(output_data, "songplays.parquet")
    songplays_table.write.partitionBy("year", "month").parquet(
        songplays_output, "overwrite"
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-kzinmr/"

    print("Performing ETL on songs JSON data...")
    process_song_data(spark, input_data, output_data)
    print("Performing ETL on events and songs JSON data...")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
