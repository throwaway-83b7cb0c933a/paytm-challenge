# Execution results from [sessionize.py](sessionize.py) 

```
(base) ubuntu@ip-10-0-0-6:~/paytm-challenge/src$ python sessionize.py
20/04/12 21:39:01 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

1. Determine the average session time.
+--------------------+                                                          
|average_session_time|
+--------------------+
|  125.66228548922523|
+--------------------+

2. Determine unique URL visits per session.
For demonstration, 20 sessions with the most number of unique URL visits are shown.
+----------------+----------------+                                             
|      session_id|unique_url_count|
+----------------+----------------+
|  52.74.219.71-5|            9532|
| 119.81.61.166-6|            8016|
| 119.81.61.166-8|            5790|
|  52.74.219.71-6|            5478|
|106.186.23.95-10|            4656|
| 119.81.61.166-1|            3334|
|  52.74.219.71-9|            2907|
| 119.81.61.166-9|            2841|
| 119.81.61.166-7|            2786|
| 106.186.23.95-5|            2731|
| 106.51.132.54-1|            2609|
|  52.74.219.71-4|            2572|
|  52.74.219.71-8|            2535|
| 52.74.219.71-10|            2466|
|  52.74.219.71-7|            2458|
|  52.74.219.71-2|            2037|
| 54.169.20.106-3|            1896|
| 119.81.61.166-5|            1739|
| 119.81.61.166-3|            1671|
|  54.169.0.163-4|            1600|
+----------------+----------------+
only showing top 20 rows

3. Find the most engaged users, i.e. the IPs with the longest session times.
For demonstration, 20 sessions with the longest durations are shown.
+---------------+-----------------+                                             
|      client_ip|max(duration_sec)|
+---------------+-----------------+
|  119.81.61.166|             2069|
|   52.74.219.71|             2069|
|  106.186.23.95|             2069|
|   125.20.39.66|             2068|
|   125.19.44.66|             2068|
| 180.211.69.209|             2067|
|   192.8.190.10|             2067|
|  54.251.151.39|             2067|
| 203.189.176.14|             2066|
| 203.191.34.178|             2066|
|  122.15.156.64|             2066|
| 180.179.213.70|             2066|
| 180.151.80.140|             2065|
|213.239.204.204|             2065|
| 103.29.159.138|             2065|
| 125.16.218.194|             2065|
|    78.46.60.71|             2064|
| 103.29.159.186|             2064|
|  192.71.175.30|             2063|
|135.245.115.245|             2063|
+---------------+-----------------+
only showing top 20 rows

WARNING:__main__:The Spark job is finished. Press Enter to terminate the program.
```
