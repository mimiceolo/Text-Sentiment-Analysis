# Real-Time Sentiment Analysis - Train Once, Predict Continuously

## Overview

**New Architecture**: Train model ONCE → Save to HDFS → Continuously predict on streaming data

This is **much more efficient** than retraining on every run!

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ ONE-TIME TRAINING (Run once or weekly)                       │
└─────────────────────────────────────────────────────────────┘

Training Data → TrainModel MapReduce → Save Model to HDFS
(/user/hadoop/kafka_data/tweets-training)  → (/user/hadoop/models/nb_model)

┌─────────────────────────────────────────────────────────────┐
│ CONTINUOUS PREDICTION (Real-time streaming)                  │
└─────────────────────────────────────────────────────────────┘

Producer (∞) → Kafka → Consumer Daemon → HDFS (time-partitioned)
                                            ↓ (hourly/minutely)
                                    /tweets-testing/2025-12-20-10/
                                    /tweets-testing/2025-12-20-11/
                                    /tweets-testing/2025-12-20-12/
                                            ↓
                                    Cron (every hour)
                                            ↓
                                PredictSentiment MapReduce
                                  (loads pre-trained model)
                                            ↓
                                    HDFS + MongoDB
                                    (predictions)
```

---

## Quick Start

### Step 1: Compile Programs

```bash
cd Hadoop
./compile_kafka_to_hdfs.sh    # Consumer
./compile_kafka_hadoop_nb.sh  # Train + Predict
```

### Step 2: Start Consumer Daemon (Testing Data Only)

```bash
# Start consumer for testing topic
./kafka_consumer_daemon.sh start \
  tweets-testing \
  /user/hadoop/kafka_data/tweets-testing \
  hourly
```

### Step 3: Start Kafka Producer (Continuous)

```bash
cd ../kafka_producer
source venv/bin/activate

# Run continuously
nohup python3 tweet_producer.py \
  --csv-file ../data/raws/training.1600000.processed.noemoticon.csv \
  --rate 100 \
  > producer.log 2>&1 &
```

### Step 4: Train Model ONCE

```bash
cd ../Hadoop

# Train on ALL training data (run once)
hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model
```

**Output**:
```
Training Naive Bayes Model
========================================
Total tweets:     150,000
Vocabulary size:  25,432
Training time:    125 seconds

✓ Model saved to: /user/hadoop/models/nb_model
```

### Step 5: Setup Scheduled Predictions

```bash
# Add to cron
crontab -e

# Run predictions every hour
0 * * * * cd /path/to/Hadoop && ./scheduled_prediction.sh >> logs/prediction_cron.log 2>&1

# OR every 5 minutes (testing)
*/5 * * * * cd /path/to/Hadoop && ./scheduled_prediction.sh >> logs/prediction_cron.log 2>&1
```

### Step 6: Monitor

```bash
# Check consumer
./kafka_consumer_daemon.sh status

# Monitor pipeline
./monitor_pipeline.sh

# Run prediction manually (test)
./scheduled_prediction.sh
```

---

## How It Works

### Training (Run Once)

**`TrainModel`** class:
1. Reads JSON data from HDFS training directory
2. Counts word occurrences per sentiment
3. Saves model to HDFS:
   - `part-r-00000, part-r-00001, ...` (word counts)
   - `_metadata.txt` (statistics)

**Model format**:
```
word1    45@12
word2    23@67
happy    89@10
...
```

**Metadata**:
```
tweets_size=150000
pos_tweets_size=75234
neg_tweets_size=74766
pos_words_size=2534523
neg_words_size=2498765
features_size=25432
```

### Prediction (Continuous)

**`PredictSentiment`** class:
1. Loads pre-trained model from HDFS in `setup()`
2. For each new tweet:
   - Cleans text
   - Calculates P(positive|tweet) and P(negative|tweet)
   - Predicts sentiment
3. Outputs predictions to HDFS
4. Writes metrics to MongoDB

**Advantages**:
- ✅ **No retraining** - Much faster
- ✅ **Model reuse** - Train once, predict millions of times
- ✅ **Real-time ready** - Quick predictions on streaming data
- ✅ **Scalable** - Handle high throughput

---

## Commands Reference

### Training

```bash
# Train on specific directory
hadoop jar kafka_hadoop_nb.jar TrainModel \
  <training_data_dir> \
  <model_output_dir>

# Example: Train on all training data
hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model

# Example: Retrain on recent data only
hadoop jar kafka_hadoop_nb.jar TrainModel \
  "/user/hadoop/kafka_data/tweets-training/2025-12-{20,21,22}*" \
  /user/hadoop/models/nb_model_recent
```

### Prediction

```bash
# Predict on specific partition
hadoop jar kafka_hadoop_nb.jar PredictSentiment \
  <model_dir> \
  <test_data_dir> \
  <output_dir>

# Example: Predict on one hour
hadoop jar kafka_hadoop_nb.jar PredictSentiment \
  /user/hadoop/models/nb_model \
  /user/hadoop/kafka_data/tweets-testing/2025-12-20-18 \
  /user/hadoop/predictions/batch_20251220_18

# Example: Predict on multiple hours
hadoop jar kafka_hadoop_nb.jar PredictSentiment \
  /user/hadoop/models/nb_model \
  "/user/hadoop/kafka_data/tweets-testing/2025-12-20-{18,19,20}" \
  /user/hadoop/predictions/batch_20251220_evening
```

### Scheduled Prediction

```bash
# Run once (processes new data only)
./scheduled_prediction.sh

# Check what would be processed
cat .prediction_state  # Last processed
hdfs dfs -ls /user/hadoop/kafka_data/tweets-testing/  # Available

# Reset state (reprocess all)
rm .prediction_state
```

---

## When to Retrain

### Option 1: Never (Use Initial Model)
- Simple, fast
- Good if data distribution doesn't change

### Option 2: Periodically (Weekly/Monthly)
```bash
# Add to cron - retrain weekly (Sunday 2 AM)
0 2 * * 0 cd /path/to/Hadoop && hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model \
  >> logs/training.log 2>&1
```

### Option 3: On Demand (When Accuracy Drops)
- Monitor accuracy in MongoDB
- If accuracy < threshold, retrain

```bash
# Example: Check recent accuracy
mongosh sentiment_analysis --eval "
  db.hadoop_prediction_batches.aggregate([
    { \$sort: {timestamp: -1} },
    { \$limit: 10 },
    { \$group: { _id: null, avg_accuracy: { \$avg: '\$accuracy' } } }
  ])
"

# If accuracy dropped, retrain
./train_model.sh  # (create this wrapper script)
```

---

## Directory Structure

```
/user/hadoop/
├── kafka_data/                        # Streaming data
│   ├── tweets-training/               # Training data (for model training)
│   │   ├── 2025-12-18-10/
│   │   ├── 2025-12-18-11/
│   │   └── ...
│   └── tweets-testing/                # Testing data (for predictions)
│       ├── 2025-12-20-10/
│       ├── 2025-12-20-11/
│       └── ...
│
├── models/                             # Trained models
│   ├── nb_model/                       # Current production model
│   │   ├── part-r-00000
│   │   ├── part-r-00001
│   │   ├── part-r-00002
│   │   └── _metadata.txt
│   └── nb_model_recent/                # Retrained model (optional)
│
└── predictions/                        # Prediction outputs
    ├── predictions_20251220-180045/
    ├── predictions_20251220-190032/
    └── ...
```

---

## Monitoring

### Check Model

```bash
# Verify model exists
hdfs dfs -test -d /user/hadoop/models/nb_model && echo "Model exists" || echo "Model not found"

# View metadata
hdfs dfs -cat /user/hadoop/models/nb_model/_metadata.txt

# Count vocabulary
hdfs dfs -cat "/user/hadoop/models/nb_model/part-r-*" | wc -l
```

### Check Predictions

```bash
# List prediction batches
hdfs dfs -ls /user/hadoop/predictions/

# View sample predictions
hdfs dfs -cat /user/hadoop/predictions/predictions_*/part-r-00000 | head -10

# Count total predictions
hdfs dfs -cat "/user/hadoop/predictions/*/part-*" | wc -l
```

### Check MongoDB

```bash
mongosh sentiment_analysis

# Count prediction batches
db.hadoop_prediction_batches.countDocuments()

# View latest batch
db.hadoop_prediction_batches.find().sort({timestamp:-1}).limit(1).pretty()

# Average accuracy over time
db.hadoop_prediction_batches.aggregate([
  { $group: { _id: null, avg_accuracy: { $avg: "$accuracy" } } }
])

# Accuracy trend (last 10 batches)
db.hadoop_prediction_batches.find(
  {}, 
  {timestamp:1, accuracy:1, tweets_processed:1, _id:0}
).sort({timestamp:-1}).limit(10)
```

---

## Performance Comparison

| Metric | Train+Test Together | Train Once, Predict Many |
|--------|---------------------|-------------------------|
| Training time | Every run (~2-5 min) | Once (~2-5 min) |
| Prediction time | N/A | Fast (~30-60 sec) |
| Total time (10 runs) | 20-50 minutes | 2-10 minutes |
| Model consistency | May vary | Consistent |
| Resource usage | High | Low |

**Example**: Process 100 batches over a week
- **Old way**: 100 trainings = 250-500 minutes
- **New way**: 1 training + 100 predictions = 5-100 minutes

**Savings**: 5-10x faster! ⚡

---

## Example Workflow

### Day 1: Setup

```bash
# 1. Start testing consumer
./kafka_consumer_daemon.sh start tweets-testing /user/hadoop/kafka_data/tweets-testing hourly

# 2. Start producer (background)
cd ../kafka_producer && source venv/bin/activate
nohup python3 tweet_producer.py --csv-file ../data/raws/training.csv --rate 100 > producer.log 2>&1 &

# 3. Train model (wait for some training data or use existing)
cd ../Hadoop
hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model

# 4. Setup cron
crontab -e
# Add: 0 * * * * cd /path/to/Hadoop && ./scheduled_prediction.sh >> logs/prediction_cron.log 2>&1
```

### Day 2-7: Automatic Operation

- Consumer runs continuously, writes to HDFS hourly
- Cron runs predictions every hour
- MongoDB accumulates results
- Monitor with `./monitor_pipeline.sh`

### Week 2: Retrain (Optional)

```bash
# Retrain with more data
hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model

# Predictions automatically use new model
```

---

## Troubleshooting

### Model not found

```bash
# Train first!
hadoop jar kafka_hadoop_nb.jar TrainModel \
  /user/hadoop/kafka_data/tweets-training \
  /user/hadoop/models/nb_model
```

### No new testing data

```bash
# Check consumer
./kafka_consumer_daemon.sh status

# Check HDFS
hdfs dfs -ls /user/hadoop/kafka_data/tweets-testing/

# Start consumer if not running
./kafka_consumer_daemon.sh start tweets-testing /user/hadoop/kafka_data/tweets-testing hourly
```

### Prediction job fails

```bash
# Check logs
tail -100 logs/prediction_*.log

# Verify model
hdfs dfs -cat /user/hadoop/models/nb_model/_metadata.txt

# Run manually to see errors
./scheduled_prediction.sh
```

---

## Summary

You now have a **production-ready real-time prediction pipeline**:

✅ **Train model once** - Fast, efficient  
✅ **Predict continuously** - Real-time streaming  
✅ **Automatic scheduling** - Cron integration  
✅ **Incremental processing** - Only new data  
✅ **Model reuse** - No redundant training  
✅ **MongoDB storage** - Metrics tracking  

**Next steps**:
1. Run `./compile_kafka_hadoop_nb.sh`
2. Train model once
3. Setup cron for predictions
4. Monitor MongoDB results!

---

**Files**:
- `TrainModel.java` - Training-only job
- `PredictSentiment.java` - Prediction-only job  
- `scheduled_prediction.sh` - Automated prediction runner
- `REAL_TIME_PREDICTION.md` - This guide
