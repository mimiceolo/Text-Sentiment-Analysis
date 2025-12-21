1. Download data from: (http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/)
2. Unzip and put data into "data/raws/" directory
3. Download all requirements from "REQUIREMENTS.txt"
4. Hadoop:

- start Hadoop (start-dfs.sh, start-yarn.sh)
- create directories
  - /user/hadoop/kafka_data/tweets-training: contains raw tranining data consumed from kafka which is used by the TrainModel job
  - /user/hadoop/kafka_data/tweets-testing: stores the streaming data for real-time predictions (PredictSentiment job)
  - /user/hadoop/models: store the trained model
  - /user/hadoop/predictions: stores the output results from PredictSentiment MapReduce job
- copy data to HDFS
  - hadoop fs -put data/raws/training.1600000.processed.noemoticon.csv /user/hadoop/kafka_data/tweets-training/
- compile model
  - cd scripts && ./compile_kafka_hadoop_nb.sh
- train model
  hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model"

- compile consumer
  - cd scripts && ./compile_consumer.sh
- start consumer
  - cd scripts && ./consumer_daemon.sh start \
    tweets-testing \
    /user/hadoop/kafka_data/tweets-testing \
    hourly
- start kafka producer
- start main prediction job
  - use crontab to run predictions every hourly

crontab -e
0 \* \* \* \* cd /full/path/to/Hadoop/scripts && ./scheduled_prediction.sh >> logs/prediction_cron.log 2>&1

5. Kafka:

- start Zookeeper (zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties)
- start Kafka server (kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties)
- create topics

# Create topic for raw tweets

kafka-topics.sh --create \
 --bootstrap-server localhost:9092 \
 --replication-factor 1 \
 --partitions 3 \
 --topic tweets-raw

# create topic for training data

kafka-topics.sh --create \
 --bootstrap-server localhost:9092 \
 --replication-factor 1 \
 --partitions 3 \
 --topic tweets-training

# create topic for testing data

kafka-topics.sh --create \
 --bootstrap-server localhost:9092 \
 --replication-factor 1 \
 --partitions 3 \
 --topic tweets-testing

# create topic for results

kafka-topics.sh --create \
 --bootstrap-server localhost:9092 \
 --replication-factor 1 \
 --partitions 3 \
 --topic sentiment-results

- start producer
  cd kafka_producer && source venv/bin/activate
  python3 tweet_producer.py \
   --csv-file ../data/raws/training.1600000.processed.noemoticon.csv \
   --rate 5

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
