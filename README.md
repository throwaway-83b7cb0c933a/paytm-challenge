# WeblogChallenge
This is an interview challenge for Paytm Labs. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

2. Predict the session length for a given IP

3. Predict the number of unique URL visits by a given IP

## Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

A. Fork this repo in github
    https://github.com/PaytmLabs/WeblogChallenge

B. Complete the processing and analytics as defined first to the best of your ability with the time provided.

C. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the Paytm Labs interview team.

D. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.



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