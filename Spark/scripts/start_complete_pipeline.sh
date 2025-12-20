#!/bin/bash

echo "========================================="
echo "Starting Complete Sentiment Analysis Pipeline"
echo "========================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to check service
check_service() {
    local SERVICE=$1
    local CHECK_CMD=$2
    
    if eval "$CHECK_CMD" &>/dev/null; then
        echo -e "${GREEN}✓${NC} $SERVICE is running"
        return 0
    else
        echo -e "${RED}✗${NC} $SERVICE is NOT running"
        return 1
    fi
}

# 1. Check/Start HDFS
echo -e "\n${YELLOW}[1/8]${NC} Checking HDFS..."
if ! check_service "HDFS" "hdfs dfs -ls /"; then
    echo "Starting HDFS..."
    start-dfs.sh
    sleep 5
fi

# 2. Check/Start YARN
echo -e "\n${YELLOW}[2/8]${NC} Checking YARN..."
if ! check_service "YARN" "yarn node -list"; then
    echo "Starting YARN..."
    start-yarn.sh
    sleep 5
fi

# 3. Check/Start Kafka
echo -e "\n${YELLOW}[3/8]${NC} Checking Kafka..."
if ! check_service "Kafka" "kafka-topics.sh --bootstrap-server localhost:9092 --list"; then
    echo "Starting Zookeeper..."
    zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
    sleep 3
    echo "Starting Kafka..."
    kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
    sleep 5
fi

# 4. Check MongoDB
# echo -e "\n${YELLOW}[4/8]${NC} Checking MongoDB..."
# if ! check_service "MongoDB" "systemctl is-active mongod"; then
#     echo "Starting MongoDB..."
#     sudo systemctl start mongod
#     sleep 2
# fi

# 5. Create HDFS directories
echo -e "\n${YELLOW}[5/8]${NC} Setting up HDFS directories..."
hdfs dfs -mkdir -p /user/spark/checkpoints &>/dev/null
hdfs dfs -mkdir -p /user/spark/checkpoints_svm &>/dev/null
hdfs dfs -mkdir -p /user/spark/streaming_results/nb &>/dev/null
hdfs dfs -mkdir -p /user/spark/streaming_results/svm &>/dev/null
hdfs dfs -mkdir -p /user/spark/models &>/dev/null
hdfs dfs -chmod -R 777 /user/spark &>/dev/null
echo -e "${GREEN}✓${NC} HDFS directories ready"

# 6. Verify Kafka topics
echo -e "\n${YELLOW}[6/8]${NC} Checking Kafka topics..."
for topic in tweets-raw tweets-training tweets-testing sentiment-results; do
    if ! kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^${topic}$"; then
        echo "Creating topic: $topic"
        kafka-topics.sh --create --bootstrap-server localhost:9092 \
          --replication-factor 1 --partitions 3 --topic $topic &>/dev/null
    fi
done
echo -e "${GREEN}✓${NC} Kafka topics ready"

# 7. Check training data
echo -e "\n${YELLOW}[7/8]${NC} Checking training data..."
if [ ! -f ../../data/preprocessed/training_processed.csv ]; then
    echo -e "${RED}✗${NC} training_processed.csv not found!"
    echo "Run: python3 preprocess_sentiment_data.py"
    exit 1
fi
echo -e "${GREEN}✓${NC} Training data exists ($(wc -l < training_processed.csv) records)"

# 8. Check if models are trained
echo -e "\n${YELLOW}[8/8]${NC} Checking trained models..."
MODEL_EXISTS=false
if hdfs dfs -test -d /user/spark/models/nb_model 2>/dev/null; then
    echo -e "${GREEN}✓${NC} NB model found"
    MODEL_EXISTS=true
else
    echo -e "${YELLOW}⚠${NC}  NB model not found at /user/spark/models/nb_model"
fi

echo ""
echo "========================================="
echo -e "${GREEN}All services are ready!${NC}"
echo "========================================="

# Display running processes
echo ""
echo "Running Java processes:"
jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager|Kafka|QuorumPeerMain|Jps" || echo "  No Hadoop/Kafka processes found"

echo ""
echo "========================================="
echo "Next Steps:"
echo "========================================="
echo ""

if [ "$MODEL_EXISTS" = false ]; then
    echo -e "${YELLOW}⚠ IMPORTANT: Train models first!${NC}"
    echo ""
    echo "Option 1: Use your existing trained model"
    echo "  - Copy your model to HDFS:"
    echo "    hdfs dfs -put /path/to/your/model /user/spark/models/nb_model"
    echo ""
    echo "Option 2: For now, test without model (will fail)"
    echo "  - You can start producer and see if data flows"
    echo ""
fi

echo "1️⃣  Start Kafka Producer (Terminal 1):"
echo "   cd kafka_producer"
echo "   python3 tweet_producer.py --csv-file ../training_processed.csv --rate 100"
echo ""

if [ "$MODEL_EXISTS" = true ]; then
    echo "2️⃣  Start Spark Streaming (Terminal 2):"
    echo "   cd Spark"
    echo "   spark-submit --class NB_Streaming --master local[4] \\"
    echo "     --driver-memory 4g --executor-memory 4g \\"
    echo "     --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 \\"
    echo "     target/scala-2.12/sentiment-analysis-streaming_2.12-1.0.jar"
    echo ""
else
    echo "2️⃣  Train model or test data flow first"
    echo ""
fi

echo "3️⃣  Monitor (Terminal 3):"
echo "   ./scripts/streaming_dashboard.sh"
echo "   OR visit: http://localhost:4040"
echo ""

echo "========================================="
echo "Useful commands:"
echo "========================================="
echo "Check status:     ./scripts/check_streaming_status.sh"
echo "Monitor Kafka:    ./scripts/monitor_kafka_lag.sh"
echo "Stop streaming:   ./scripts/stop_streaming.sh"
echo "========================================="
