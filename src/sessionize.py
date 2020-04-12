"""
This PySpark logic makes basic analytical observations about an AWS Elastic Load Balancer.

Author:
David Lee
"""

import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import coalesce, col, concat, lag, lit, regexp_extract, sum, unix_timestamp


def get_base_log(spark, filename):
    """Load and parse the ELB log for timestamp, client IP, and requested URL."""
    base_df = spark.read.text(filename)

    # For the Elastic Load Balancer log entry syntax, refer to:
    # https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
    weblog_pattern = r"^(\S+Z) \S*? (\d+\.\d+\.\d+\.\d+):\d+ .+? \"\w+ (.*?) "
    logs_df = base_df.select(
        regexp_extract("value", weblog_pattern, 1).cast("timestamp").alias("ts_utc"),
        regexp_extract("value", weblog_pattern, 2).cast("string").alias("client_ip"),
        regexp_extract("value", weblog_pattern, 3).cast("string").alias("request_url"),
    ).repartition("client_ip")

    return logs_df


def sessionize_log(logs_df, max_session_in_seconds):
    """Label each request with a session ID consisting of client IP and a session label.
    
    Strategy: over a timestamp-sorted window partitioned by client_ip:
    1. calculate the time difference between the current and the previous event
    2. if the difference is greater than the session time limit, indicate it with number 1; otherwise, 0.
    3. calculate the cumulative sum of the above, which becomes a session number the each IP address.
    4. combine the session number with the client IP to make each session label unique.
    
    Inspired by https://randyzwitch.com/sessionizing-log-data-sql/ ."""

    session_window = Window.partitionBy("client_ip").orderBy("ts_utc")
    working_df = (
        logs_df.withColumn("ts_prev", lag("ts_utc", 1).over(session_window))
        .withColumn(
            "new_session",
            (
                (unix_timestamp("ts_utc") - coalesce(unix_timestamp("ts_prev"), lit(0))) > max_session_in_seconds
            ).cast("integer"),
        )
        .withColumn("session_no", sum("new_session").over(session_window))
        .withColumn("session_id", concat(col("client_ip"), lit("-"), col("session_no").cast("string")))
    )

    session_df = working_df.select("ts_utc", "client_ip", "request_url", "session_id").cache()

    return session_df


def analyze_log(spark, session_df):
    """Analyze the logs to answer the coding challenge questions."""
    lines_for_preview = 20

    # Calculate the duration and number of unique URL visits per session.
    # Herein, a session is defined with the following rules:
    #   1. A session must contain AT LEAST two events.
    #   2. Session length is defined by the difference between the first and the last timestamp.
    #     --> If a client requested two pages on the same second, this session will have a duration of 0 second.
    #   3. All sessions expire at the end of the log.

    session_df.createOrReplaceTempView("logs")
    analysis_df = spark.sql(
        """SELECT
        client_ip,
        session_id, 
        max(ts_utc) AS end_time, 
        min(ts_utc) AS start_time, 
        unix_timestamp(max(ts_utc)) - unix_timestamp(min(ts_utc)) AS duration_sec,
        COUNT(DISTINCT request_url) AS unique_url_count
    FROM logs
    GROUP BY 
        client_ip, 
        session_id
    HAVING
        COUNT(request_url) > 1
    ORDER BY
        duration_sec DESC"""
    )
    analysis_df.createOrReplaceTempView("log_analysis")

    # 1. Determine the average session time
    print("1. Determine the average session time.")
    spark.sql("""SELECT avg(duration_sec) as average_session_time FROM log_analysis""").show(1)

    # 2. Determine unique URL visits per session.
    print("2. Determine unique URL visits per session.")
    print(f"For demonstration, {lines_for_preview} sessions with the most number of unique URL visits are shown.")
    spark.sql(
        """SELECT session_id, unique_url_count FROM log_analysis
        ORDER BY unique_url_count DESC"""
    ).show(lines_for_preview)

    # 3. Find the most engaged users, i.e. the IPs with the longest session times.
    #    Here, duplicates should be eliminated:
    #    e.g. if a particular client were to have ranked #1 and #3, show it only once.
    print("3. Find the most engaged users, i.e. the IPs with the longest session times.")
    print(f"For demonstration, {lines_for_preview} sessions with the longest durations are shown.")
    spark.sql(
        """SELECT client_ip, max(duration_sec) FROM log_analysis
        GROUP BY client_ip ORDER BY max(duration_sec) DESC"""
    ).show(lines_for_preview)


def main(logfile_path, max_session_in_seconds):

    # Initialize Spark SQL session.
    logger.info("Instantiating a Spark Session...")
    spark = SparkSession.builder.appName("paytm-sessionize-weblog-v1").getOrCreate()

    logger.info("Loading the web log...")
    logs_df = get_base_log(spark, logfile_path)

    logger.info("Sessionizing the web log...")
    session_df = sessionize_log(logs_df, max_session_in_seconds)

    logger.info("Analyzing the sessionized web log...")
    analyze_log(spark, session_df)

    logger.warning("The Spark job is finished. Press Enter to terminate the program.")
    _ = input()


if __name__ == "__main__":
    # Define the session timeout as 15 minutes * 60 seconds/minute = 900 seconds
    max_session_in_seconds = 900
    # Location of the ELB log
    logfile_path = "../data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

    # Create a logging facility
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.WARNING)

    main(logfile_path, max_session_in_seconds)
