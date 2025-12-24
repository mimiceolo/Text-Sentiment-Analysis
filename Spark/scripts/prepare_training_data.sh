#!/bin/bash

echo "=================================================="
echo "Preparing Training Data for Spark Models"
echo "=================================================="

# Check if HDFS is running
if ! jps | grep -q "NameNode"; then
    echo "❌ Error: HDFS NameNode is not running"
    echo "Start HDFS with: start-dfs.sh"
    exit 1
fi

echo "✓ HDFS is running"

# Create directories
echo ""
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /user/spark/models
hdfs dfs -mkdir -p /user/kafka_data/tweets-training
hdfs dfs -mkdir -p /user/preprocessed

echo "✓ Directories created"

# Check if training data exists
echo ""
echo "Checking for training data..."

# Option 1: Check for CSV data from Kafka
if hdfs dfs -test -e /user/kafka_data/tweets-training/_SUCCESS 2>/dev/null; then
    echo "✓ Found Kafka training data"
    DATA_SOURCE="kafka"
# Option 2: Check for local CSV
elif [ -f "../../data/preprocessed/training_processed.csv" ]; then
    echo "✓ Found local processed CSV"
    echo "Uploading to HDFS..."
    hdfs dfs -put ../../data/preprocessed/training_processed.csv /user/kafka_data/tweets-training/
    DATA_SOURCE="csv"
# Option 3: Check for original CSV
elif [ -f "../../data/raws/training.1600000.processed.noemoticon.csv" ]; then
    echo "⚠ Found original CSV (needs preprocessing)"
    echo ""
    echo "You need to preprocess the data first. Options:"
    echo "1. Run: python3 preprocess_sentiment_data.py"
    echo "2. OR use Kafka producer to stream data"
    echo "3. OR run the Preprocessing.ipynb notebook"
    exit 1
else
    echo "❌ No training data found!"
    echo ""
    echo "Please provide training data:"
    echo "1. Place training.1600000.processed.noemoticon.csv in data/raws/"
    echo "2. OR run Kafka producer to populate /user/kafka_data/tweets-training/"
    exit 1
fi

echo ""
echo "=================================================="
echo "Data Preparation Complete!"
echo "=================================================="
