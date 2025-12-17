# MongoDB Integration Guide for Sentiment Analysis Pipeline

## 2. MongoDB Setup

### 2.3 Create Database and Collections

```bash
# Connect to MongoDB
mongosh

# Switch to database
use sentiment_analysis

# Create collections with validation
db.createCollection("predictions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["tweet_id", "text", "true_label", "predicted_label", "model", "timestamp"],
      properties: {
        tweet_id: { bsonType: "string" },
        text: { bsonType: "string" },
        true_label: { bsonType: "int" },
        predicted_label: { bsonType: "int" },
        confidence: { bsonType: "double" },
        model: { bsonType: "string", enum: ["naive_bayes", "svm", "hadoop_nb", "hadoop_modified_nb"] },
        batch_id: { bsonType: "string" },
        timestamp: { bsonType: "date" }
      }
    }
  }
})

# Create collection for batch metrics
db.createCollection("batch_metrics", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["batch_id", "model", "accuracy", "timestamp"],
      properties: {
        batch_id: { bsonType: "string" },
        model: { bsonType: "string" },
        accuracy: { bsonType: "double" },
        f1_score: { bsonType: "double" },
        precision: { bsonType: "double" },
        recall: { bsonType: "double" },
        total_tweets: { bsonType: "int" },
        processing_time_ms: { bsonType: "long" },
        timestamp: { bsonType: "date" }
      }
    }
  }
})

# Create collection for aggregated sentiment trends
db.createCollection("sentiment_trends")

# Create indexes for performance
db.predictions.createIndex({ "timestamp": -1 })
db.predictions.createIndex({ "model": 1, "timestamp": -1 })
db.predictions.createIndex({ "batch_id": 1 })
db.predictions.createIndex({ "tweet_id": 1 }, { unique: true })
db.batch_metrics.createIndex({ "model": 1, "timestamp": -1 })
db.sentiment_trends.createIndex({ "timestamp": -1 })

# Create a time-series collection (MongoDB 5.0+) for metrics
db.createCollection("realtime_metrics", {
  timeseries: {
    timeField: "timestamp",
    metaField: "model",
    granularity: "seconds"
  }
})

exit
```

## 6. Complete Implementation

```bash
#!/bin/bash

# Terminal 1: Start Kafka Producer
cd kafka_producer
python3 tweet_producer.py \
  --csv-file ../training_processed.csv \
  --rate 100 \
  --train-ratio 0.8

# Terminal 2: Start Spark Streaming NB with MongoDB
spark-submit \
  --class NB_Streaming_MongoDB \
  --master "local[4]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --packages \
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,\
org.mongodb:mongodb-driver-sync:4.11.1 \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

# Terminal 3: Start Spark Streaming SVM with MongoDB
spark-submit \
  --class SVM_Streaming_MongoDB \
  --master "local[4]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --packages \
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,\
org.mongodb:mongodb-driver-sync:4.11.1 \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

```

### 6.3 Start API and Dashboard

```bash
# Terminal 4: Start Flask API
cd api
pip install flask flask-cors pymongo
python3 sentiment_api.py

# Terminal 5: Start Dashboard (open in browser)
# Visit: http://localhost:5000/api/health
# Dashboard: Create separate frontend or use the HTML above
```

---

## 7. Performance Optimization

### 7.1 MongoDB Indexing Strategy

```javascript
// Connect to MongoDB
mongosh sentiment_analysis

// Create compound indexes for common queries
db.predictions.createIndex({ "model": 1, "timestamp": -1 })
db.predictions.createIndex({ "batch_id": 1 })
db.predictions.createIndex({ "predicted_label": 1, "timestamp": -1 })
db.predictions.createIndex({ "correct": 1, "model": 1 })

// Create text index for search
db.predictions.createIndex({ "text": "text" })

// Create TTL index to auto-delete old data (optional)
db.predictions.createIndex(
  { "timestamp": 1 },
  { expireAfterSeconds: 2592000 }  // 30 days
)

// Analyze index usage
db.predictions.aggregate([
  { $indexStats: {} }
])
```

### 7.2 Spark Optimization

Update `Spark/build.sbt` to use assembly plugin:

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
```

Create `Spark/project/assembly.sbt`:

```scala
// Assembly merge strategy
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

### 7.3 Batch Write Optimization

Modify MongoDB writes to use bulk operations:

```scala
// Instead of writing row by row, batch writes
predictionsForMongo
  .write
  .format("mongodb")
  .mode(SaveMode.Append)
  .option("collection", "predictions")
  .option("maxBatchSize", 1000)  // Batch size
  .option("ordered", "false")     // Unordered for better performance
  .save()
```
