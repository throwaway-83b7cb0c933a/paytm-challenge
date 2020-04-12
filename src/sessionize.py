import logging
import os
from time import sleep

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import coalesce, col, concat, lag, lit, regexp_extract, sum, unix_timestamp 


def get_base_log(spark, filename):
    """Load and parse the ELB log for timestamp, client IP, and requested URL."""
    base_df = spark.read.text(filename)

    weblog_pattern = r"^(\S+Z) \S*? (\d+\.\d+\.\d+\.\d+):\d+ .+? \"\w+ (.*?) "
    logs_df = base_df \
        .select(
            regexp_extract('value', weblog_pattern, 1).cast('timestamp').alias('ts_utc'),
            regexp_extract('value', weblog_pattern, 2).cast('string').alias('client_ip'),
            regexp_extract('value', weblog_pattern, 3).cast('string').alias('request_url')) \
        .repartition("client_ip")

    return logs_df


def sessionize_log(logs_df, max_session_in_seconds):
    """Label each request with a session ID consisting of client IP and a session number"""
    session_window = Window.partitionBy("client_ip").orderBy("ts_utc")
    working_df = logs_df \
        .withColumn("ts_prev", lag("ts_utc", 1).over(session_window)) \
        .withColumn("new_session", ((unix_timestamp("ts_utc") - coalesce(unix_timestamp("ts_prev"), lit(0))) > max_session_in_seconds).cast("integer")) \
        .withColumn("session_no", sum("new_session").over(session_window)) \
        .withColumn("session_id", concat(col("client_ip"), lit("-"), col("session_no").cast("string")))

    session_df = working_df \
        .select("ts_utc", "client_ip", "request_url", "session_id") \
        .cache()

    return session_df


def analyze_log(spark, session_df):
    """Analyze the logs to answer the questions given by the challenge."""
    lines_for_preview = 50

    session_df.createOrReplaceTempView("logs")
    analysis_df = spark.sql("""SELECT
        client_ip,
        session_id, 
        max(ts_utc) AS end_time, 
        min(ts_utc) AS start_time, 
        unix_timestamp(max(ts_utc)) - unix_timestamp(min(ts_utc)) + 1 AS duration_sec,
        COUNT(DISTINCT request_url) AS unique_url_count
    FROM logs
    GROUP BY 
        client_ip, 
        session_id
    ORDER BY
        duration_sec DESC""")
    analysis_df.createOrReplaceTempView("log_analysis")
    
    # 1. Determine the average session time
    print("1. Determine the average session time.")
    spark.sql("""SELECT avg(duration_sec) as average_session_time FROM log_analysis""").show(1)

    # 2. Determine unique URL visits per session.
    print("2. Determine unique URL visits per session.")
    print(f"For demonstration, {lines_for_preview} sessions with the most number of unique URL visits are shown.")
    spark.sql("""SELECT session_id, unique_url_count FROM log_analysis
        ORDER BY unique_url_count DESC""").show(lines_for_preview)
    
    # 3. Find the most engaged users, i.e. the IPs with the longest session times.
    print("3. Find the most engaged users, i.e. the IPs with the longest session times.")
    print(f"For demonstration, {lines_for_preview} sessions with the most number of unique URL visits are shown.")
    spark.sql("""SELECT client_ip, duration_sec FROM log_analysis
        ORDER BY duration_sec DESC""").show(lines_for_preview)


def main(logfile_path, max_session_in_seconds):

    # Initialize Spark SQL session.
    logger.info("Instantiating a Spark Session...")
    spark = SparkSession \
        .builder \
        .appName("paytm-sessionize-weblog-v1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Loading the web log...")
    logs_df = get_base_log(spark, logfile_path)

    logger.info("Sessionizing the web log...")
    session_df = sessionize_log(logs_df, max_session_in_seconds)

    logger.info("Analyzing the sessionized web log...")
    analyze_log(spark, session_df)
    
    logger.warning("The Spark job is finished. Press Enter to terminate the program.")
    _ = input()


if __name__ == "__main__":
    # ELB log
    logfile_path = "../data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    # Define the session timeout as 15 minutes * 60 seconds/minute = 900 seconds
    max_session_in_seconds = 900
    # Create a logging facility
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    main(logfile_path, max_session_in_seconds)
