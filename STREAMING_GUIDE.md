# Streaming Sentiment Analysis with MongoDB - Quick Start Guide

Simple guide to run NB/SVM Streaming applications with MongoDB from scratch.

---

## Prerequisites

Make sure you have installed:
- **Hadoop** (HDFS running on port 9000)
- **Spark** (version 3.x)
- **Kafka** (running on port 9092)
- **MongoDB** (running on port 27017)
- **Python 3** (with kafka-python)
- **Scala/sbt** (for building Spark applications)

---

## Step 1: Prepare Your Data

### 1.1 Get the Dataset

Download the Sentiment140 dataset:
```bash
# Place your dataset here
data/raws/training.1600000.processed.noemoticon.csv
```

### 1.2 Upload Raw Data to HDFS

```bash
# Create HDFS directories
hdfs dfs -mkdir -p /user/kafka_data/tweets-training

# Upload the CSV file to HDFS
hdfs dfs -put data/raws/training.1600000.processed.noemoticon.csv \
  /user/kafka_data/tweets-training/
```

---

## Step 2: Train the Models

### 2.1 Build the Spark Application

```bash
cd Spark
sbt package
```

### 2.2 Train Both NB and SVM Models

```bash
# Train models (this will read from HDFS and save models to HDFS)
spark-submit \
  --class TrainModels \
  --master local[*] \
  target/scala-2.12/sentiment-analysis_2.12-1.0.jar
```

This creates:
- `hdfs://localhost:9000/user/spark/models/nb_model`
- `hdfs://localhost:9000/user/spark/models/svm_model`

### 2.3 Create Checkpoint Directories

```bash
hdfs dfs -mkdir -p /user/spark/checkpoints_nb_mongo
hdfs dfs -mkdir -p /user/spark/checkpoints_svm_mongo
hdfs dfs -mkdir -p /user/spark/streaming_results/nb
hdfs dfs -mkdir -p /user/spark/streaming_results/svm
```

---

## Step 3: Set Up MongoDB

### 3.1 Start MongoDB

```bash
# Start MongoDB service
sudo systemctl start mongod

# Or if using Docker
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### 3.2 Create Database and Collections

```bash
# Connect to MongoDB
mongosh

# Create database and collections
use sentiment_analysis

db.createCollection("predictions")
db.createCollection("batch_metrics")

# Create indexes for better performance
db.predictions.createIndex({ "batch_id": 1 })
db.predictions.createIndex({ "model": 1 })
db.predictions.createIndex({ "timestamp": -1 })
db.batch_metrics.createIndex({ "batch_id": 1 })
db.batch_metrics.createIndex({ "model": 1 })
db.batch_metrics.createIndex({ "timestamp": -1 })

exit
```

---

## Step 4: Set Up Kafka

### 4.1 Start Kafka

```bash
# Start Zookeeper (if not already running)
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka (in a new terminal)
kafka-server-start.sh config/server.properties
```

### 4.2 Create Kafka Topics

```bash
# Create the testing topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic tweets-testing \
  --partitions 3 \
  --replication-factor 1

# Verify topic creation
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Step 5: Run the Streaming Applications

### Option A: Run Naive Bayes Streaming

```bash
cd Spark

spark-submit \
  --class NB_Streaming_MongoDB \
  --master local[*] \
  --packages org.mongodb:mongodb-driver-sync:4.11.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/scala-2.12/sentiment-analysis_2.12-1.0.jar
```

### Option B: Run SVM Streaming

```bash
cd Spark

spark-submit \
  --class SVM_Streaming_MongoDB \
  --master local[*] \
  --packages org.mongodb:mongodb-driver-sync:4.11.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/scala-2.12/sentiment-analysis_2.12-1.0.jar
```

### Option C: Run Both (Different Terminals)

Open two terminals and run both commands above simultaneously.

---

## Step 6: Stream Test Data with Kafka Producer

### 6.1 Install Python Dependencies

```bash
cd kafka_producer
pip install kafka-python
```

### 6.2 Start Streaming Tweets

```bash
# Stream tweets to Kafka (testing topic)
python3 tweet_producer.py \
  --csv-file ../data/raws/training.1600000.processed.noemoticon.csv \
  --rate 50
```

**Note:** The producer sends tweets to both `tweets-training` and `tweets-testing` topics. The streaming apps consume from `tweets-testing`.

---

## Step 7: Monitor Results

### 7.1 Check Console Output

Watch the Spark console for real-time metrics:
```
[NB-MongoDB] Processing batch abc-123 with 500 records
[NB-MongoDB] Batch abc-123 - Accuracy: 0.76, F1: 0.75
[NB-MongoDB] ✓ Wrote 500 predictions to MongoDB
[NB-MongoDB] ✓ Batch complete in 2345ms
```

### 7.2 Query MongoDB

```bash
mongosh

use sentiment_analysis

# Count predictions
db.predictions.countDocuments()

# View recent predictions
db.predictions.find().limit(5).pretty()

# Check batch metrics
db.batch_metrics.find().sort({ timestamp: -1 }).limit(5).pretty()

# Get accuracy by model
db.batch_metrics.aggregate([
  { $group: { 
    _id: "$model", 
    avg_accuracy: { $avg: "$accuracy" },
    avg_f1: { $avg: "$f1_score" }
  }}
])
```

### 7.3 Check HDFS Backup

```bash
# View parquet files (backup)
hdfs dfs -ls /user/spark/streaming_results/nb/
hdfs dfs -ls /user/spark/streaming_results/svm/
```

---

## Quick Commands Reference

### Start Everything

```bash
# Terminal 1: Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Kafka
kafka-server-start.sh config/server.properties

# Terminal 3: MongoDB
sudo systemctl start mongod

# Terminal 4: NB Streaming
cd Spark && spark-submit --class NB_Streaming_MongoDB --master local[*] \
  --packages org.mongodb:mongodb-driver-sync:4.11.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/scala-2.12/sentiment-analysis_2.12-1.0.jar

# Terminal 5: SVM Streaming
cd Spark && spark-submit --class SVM_Streaming_MongoDB --master local[*] \
  --packages org.mongodb:mongodb-driver-sync:4.11.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/scala-2.12/sentiment-analysis_2.12-1.0.jar

# Terminal 6: Producer
cd kafka_producer && python3 tweet_producer.py \
  --csv-file ../data/raws/training.1600000.processed.noemoticon.csv --rate 50
```

### Stop Everything

```bash
# Stop Kafka producer (Ctrl+C)
# Stop Spark streaming apps (Ctrl+C)
# Stop Kafka
kafka-server-stop.sh

# Stop Zookeeper
zookeeper-server-stop.sh

# Stop MongoDB
sudo systemctl stop mongod
```

---

## Troubleshooting

### Issue: Kafka connection refused
```bash
# Check if Kafka is running
netstat -an | grep 9092

# Restart Kafka
kafka-server-stop.sh
kafka-server-start.sh config/server.properties
```

### Issue: MongoDB connection failed
```bash
# Check MongoDB status
sudo systemctl status mongod

# Check MongoDB logs
sudo tail -f /var/log/mongodb/mongod.log
```

### Issue: HDFS connection refused
```bash
# Start HDFS
start-dfs.sh

# Check if namenode is running
jps | grep NameNode
```

### Issue: Model not found
```bash
# Check if models exist
hdfs dfs -ls /user/spark/models/

# If not, retrain models
cd Spark && spark-submit --class TrainModels --master local[*] \
  target/scala-2.12/sentiment-analysis_2.12-1.0.jar
```

### Issue: Spark memory error
```bash
# Increase executor memory
spark-submit --executor-memory 4G --driver-memory 2G \
  --class NB_Streaming_MongoDB ...
```

---

## Architecture Overview

```
CSV Data
   ↓
[Kafka Producer] → tweets-testing topic
   ↓
[Spark Streaming Consumer]
   ├─ NB_Streaming_MongoDB  (Naive Bayes)
   └─ SVM_Streaming_MongoDB (SVM)
   ↓
[Predictions + Metrics]
   ├─ MongoDB (predictions & batch_metrics collections)
   └─ HDFS (parquet backup)
```

---

## What Gets Stored in MongoDB

### predictions Collection
```json
{
  "tweet_id": "1467810369",
  "text": "miss you already",
  "true_label": 0,
  "predicted_label": 0,
  "model": "naive_bayes",
  "batch_id": "abc-123-def",
  "timestamp": "2025-12-17T10:30:00Z",
  "correct": true
}
```

### batch_metrics Collection
```json
{
  "batch_id": "abc-123-def",
  "model": "naive_bayes",
  "accuracy": 0.76,
  "f1_score": 0.75,
  "precision": 0.77,
  "recall": 0.74,
  "total_tweets": 500,
  "processing_time_ms": 2345,
  "timestamp": "2025-12-17T10:30:00Z"
}
```

---

## Next Steps

1. **Dashboard**: Build a web dashboard to visualize results from MongoDB
2. **Real-time Monitoring**: Use MongoDB Charts or Grafana
3. **Model Comparison**: Compare NB vs SVM performance in real-time
4. **Alerts**: Set up alerts for low accuracy batches
5. **Scaling**: Increase Kafka partitions and Spark executors for higher throughput

---

## Additional Resources

- **MongoDB Query Examples**: `docs/mongodb/docs.md`
- **Kafka Configuration**: `docs/kafka/`
- **Full Project Documentation**: `README.md`

---

**That's it!** You now have a complete streaming sentiment analysis pipeline with MongoDB storage. 🚀
