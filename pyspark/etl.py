import configparser
import logging.config
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, hour, dayofmonth, weekofyear, month, year, dayofweek, \
    monotonically_increasing_id, col

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read('dl.cfg')

# No need to set env variables (just for testing)
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create spark session
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    create_spark_context(spark)
    return spark


def create_spark_context(spark):
    """
    hadoop configuration to connect to AWS s3a
    :param spark:
    :return:
    """
    sc = spark.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    hadoop_conf.set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")


def process_song_data(spark):
    """
    Description:
        Process song data files and create extract songs table and artist table data from it.
    :param spark:
    :return:
    """
    input_data = config['AWS']['INPUT_DATA_SD']
    output_data = config['AWS']['OUTPUT_DATA']
    # output_data = '/tmp/parquet/'

    # read song data file
    df = spark.read.json(input_data,
                         mode="PERMISSIVE",
                         columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs/', mode="overwrite", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artist_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                             "artist_longitude").drop_duplicates()

    # write artists table to parquet files
    artist_table.write.parquet(output_data + 'artist/', mode="overwrite")


def process_log_data(spark):
    """
    Description: Process the event log file and extract data for table time, users and songplays
    :param spark:
    :return:
    """
    log_data = config['AWS']['INPUT_DATA_LD']
    output_data = config['AWS']['OUTPUT_DATA']
    # output_data = '/tmp/parquet/'

    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    users_table = df.select("userId", "firstName", "lastName", "gender", "level").drop_duplicates()

    users_table.write.parquet(output_data + 'users/', mode="overwrite")

    # create start_time column from original epoch column
    df = df.withColumn('start_time', from_unixtime(df.ts / 1000).cast('timestamp').alias('start_time'))

    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")).\
                    withColumn("day", dayofmonth("start_time")).\
                    withColumn("week", weekofyear("start_time")).\
                    withColumn("month", month("start_time")).\
                    withColumn("year", year("start_time")).\
                    withColumn("weekday", dayofweek("start_time")).\
                    select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    time_table.write.parquet(output_data + "time_table/", mode='overwrite', partitionBy=["year", "month"])

    song_df = spark.read.parquet(output_data+'songs/*/*/')
    song_df.explain()

    song_df = spark.read \
        .format("parquet") \
        .option("basePath", os.path.join(output_data, "songs/")) \
        .load(os.path.join(output_data, "songs/*/*/"))

    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner") \
        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id",
                "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite",
                                                    partitionBy=["year", "month"])


def main():
    logger.debug("Starting up...")
    spark = create_spark_session()

    logger.debug("Processing Song data")
    process_song_data(spark)
    logger.debug("Processing Log data")
    process_log_data(spark)

    spark.stop()
    logger.debug("ELT process completed")


if __name__ == "__main__":
    main()
