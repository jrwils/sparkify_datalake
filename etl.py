import configparser
from datetime import datetime
import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


@udf(TimestampType())
def convert_datetime(ts):
    """
    A user-defined function to convert Epoch time (in milliseconds)
    to a Timestamp object.
    """
    return datetime.fromtimestamp(ts / 1000)


@udf
def random_uuid():
    """
    A user-defined function to create a random UUID. This is used to
    insert into the songplays table for the songplay_id requirement.
    """
    return uuid.uuid4()


def create_spark_session():
    """
    Create the Spark session instance for use throughout the entire process.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function extracts data from S3, filters the relevant columns,
    and writes parquet files back to S3 for both song and artist data.
    """
    # Read the song_data json files from S3.
    song_data = os.path.join(input_data, 'song_data/A/*/*/*.json')
    df = spark.read.json(song_data)

    # Extract the relevant columns to create the songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    )

    # Write songs table to parquet files on S3,
    # partitioned by year and artist
    songs_output = os.path.join(output_data, 'songs.parquet')
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(songs_output)

    # Extract the relevant columns to create the artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    )

    # Write the artists table to parquet files on S3
    artists_output = os.path.join(output_data, 'artists.parquet')
    artists_table.write \
        .mode("overwrite") \
        .parquet(artists_output)


def process_log_data(spark, input_data, output_data):
    """
    This function reads log data from S3, filters the relevant columns,
    and writes the users, time and songplays tables to S3.
    """
    # Get the filepath to the log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # Read the log data file into a DataFrame
    df = spark.read.json(log_data)

    # Filter out unneeded rows
    df = df.filter(df['page'] == 'NextSong')

    # Extract the columns for the users table
    users_table = df.select(
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ).distinct()

    # Write the users table to parquet files
    users_table.write \
        .mode("overwrite") \
        .parquet(os.path.join(output_data, 'users.parquet'))

    # Create a timestamp column from the original ts column
    df = df.withColumn("ts", convert_datetime("ts"))

    # Extract columns to create the time table
    time_table = df.selectExpr(
        "ts as start_time",
        "hour(ts) as hour",
        "day(ts) as day",
        "weekofyear(ts) as week",
        "month(ts) as month",
        "year(ts) as year",
        "dayofweek(ts) as weekday"
    )

    # Write the time table to parquet files partitioned by year and month
    time_table.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(os.path.join(output_data, "time.parquet"))

    # Read in the song data from S3 to use for songplays table
    song_data_path = os.path.join(output_data, "songs.parquet")
    song_df = spark.read.parquet(song_data_path)

    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")

    # Extract columns from joined song and log datasets to create songplays
    songplays_table = spark.sql(
        """
        SELECT
            logs.ts as start_time,
            logs.userId as user_id,
            logs.level,
            songs.song_id,
            songs.artist_id,
            logs.sessionId as session_id,
            logs.location,
            logs.userAgent as user_agent,
            year(logs.ts) as year,
            month(logs.ts) as month
        FROM logs
        INNER JOIN songs on (logs.song = songs.title)
        """
    )

    # Add a songplay_id to uniquely identify a songplay
    songplays_table = songplays_table.withColumn(
        "songplay_id",
        random_uuid()
    )

    # Write songplays table to parquet files partitioned by year and month
    songplays_table.write \
        .partitionBy(
            "year",
            "month"
        ).mode("overwrite") \
        .parquet(os.path.join(output_data, "songplays.parquet"))


def main():
    """
    Runs the entire Spark job
    """
    spark = create_spark_session()
    s3_bucket = "s3a://jbucket7804216/sparkify/"
    input_data = os.path.join(s3_bucket, 'input')
    output_data = os.path.join(s3_bucket, 'output')

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
