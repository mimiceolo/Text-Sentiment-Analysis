1. Download data from: (http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/)
2. Unzip and put data into "data/raws/" directory
3. Download all requirements from "REQUIREMENTS.txt"
4. Hadoop:

- start Hadoop
- create directories
- copy data to HDFS
- compile and run MapReduce jobs

5. Kafka:

- start Zookeeper
- start Kafka
- create topics

6. MongoDB:
   - start MongoDB
   - create database and collections
7. Spark:
   - compile and run Spark jobs (sbt clean compile package)
8. Start dashboard and api (cd api && python3 sentiment_api.py)

NOTE:

1. kafka UI: http://localhost:8080/
2. hdfs UI: http://localhost:9870/
3. Spark jobs: http://localhost:4040/jobs/
4. hadoop: http://localhost:8088/
5. dashboard: http://localhost:5000/dashboard
