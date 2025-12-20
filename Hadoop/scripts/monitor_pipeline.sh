#!/bin/bash

# Monitor Continuous Kafka-Hadoop Pipeline
# Shows real-time statistics and status

cd ..

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Configuration
TRAINING_BASE="/user/hadoop/kafka_data/tweets-training"
TESTING_BASE="/user/hadoop/kafka_data/tweets-testing"
CONSUMER_PID_FILE="$SCRIPT_DIR/kafka_consumer.pid"
CONSUMER_LOG="$SCRIPT_DIR/kafka_consumer.log"
MAPREDUCE_STATE="$SCRIPT_DIR/.mapreduce_state"

clear

echo "=========================================="
echo "  Kafka-Hadoop Pipeline Monitor"
echo "=========================================="
echo ""

# 1. Check Kafka Consumer Status
echo -e "${CYAN}[1] Kafka Consumer Status${NC}"
echo "----------------------------------------"

if [ -f "$CONSUMER_PID_FILE" ]; then
    PID=$(cat "$CONSUMER_PID_FILE")
    if kill -0 $PID 2>/dev/null; then
        echo -e "${GREEN}✓ Running${NC} (PID: $PID)"
        
        # Get process info
        UPTIME=$(ps -p $PID -o etime= | tr -d ' ')
        MEM=$(ps -p $PID -o %mem= | tr -d ' ')
        CPU=$(ps -p $PID -o %cpu= | tr -d ' ')
        
        echo "  Uptime: $UPTIME"
        echo "  Memory: ${MEM}%"
        echo "  CPU:    ${CPU}%"
        
        # Parse recent activity from log
        if [ -f "$CONSUMER_LOG" ]; then
            RECENT=$(tail -20 "$CONSUMER_LOG" | grep -E "Batch|Rate:" | tail -3)
            if [ ! -z "$RECENT" ]; then
                echo ""
                echo "  Recent activity:"
                echo "$RECENT" | sed 's/^/    /'
            fi
        fi
    else
        echo -e "${RED}✗ Not running${NC} (stale PID)"
    fi
else
    echo -e "${RED}✗ Not running${NC}"
fi

echo ""

# 2. HDFS Data Statistics
echo -e "${CYAN}[2] HDFS Data Statistics${NC}"
echo "----------------------------------------"

echo "Training Data:"
TRAIN_PARTITIONS=$(hdfs dfs -ls "$TRAINING_BASE" 2>/dev/null | grep "^d" | wc -l)
TRAIN_FILES=$(hdfs dfs -ls -R "$TRAINING_BASE" 2>/dev/null | grep "^-" | wc -l)
TRAIN_SIZE=$(hdfs dfs -du -s "$TRAINING_BASE" 2>/dev/null | awk '{print $1}')
TRAIN_SIZE_MB=$(echo "scale=2; $TRAIN_SIZE / 1048576" | bc 2>/dev/null)

echo "  Partitions: $TRAIN_PARTITIONS"
echo "  Files:      $TRAIN_FILES"
echo "  Size:       ${TRAIN_SIZE_MB} MB"

# Show recent partitions
RECENT_TRAIN=$(hdfs dfs -ls "$TRAINING_BASE" 2>/dev/null | grep "^d" | awk '{print $NF}' | xargs -n1 basename | sort | tail -5)
if [ ! -z "$RECENT_TRAIN" ]; then
    echo "  Latest 5:"
    echo "$RECENT_TRAIN" | sed 's/^/    /'
fi

echo ""
echo "Testing Data:"
TEST_PARTITIONS=$(hdfs dfs -ls "$TESTING_BASE" 2>/dev/null | grep "^d" | wc -l)
TEST_FILES=$(hdfs dfs -ls -R "$TESTING_BASE" 2>/dev/null | grep "^-" | wc -l)
TEST_SIZE=$(hdfs dfs -du -s "$TESTING_BASE" 2>/dev/null | awk '{print $1}')
TEST_SIZE_MB=$(echo "scale=2; $TEST_SIZE / 1048576" | bc 2>/dev/null)

echo "  Partitions: $TEST_PARTITIONS"
echo "  Files:      $TEST_FILES"
echo "  Size:       ${TEST_SIZE_MB} MB"

echo ""

# 3. MapReduce Job Status
echo -e "${CYAN}[3] MapReduce Status${NC}"
echo "----------------------------------------"

if [ -f "$MAPREDUCE_STATE" ]; then
    LAST_PROCESSED=$(cat "$MAPREDUCE_STATE")
    echo "Last processed: $LAST_PROCESSED"
    
    # Calculate pending partitions
    PENDING_TRAIN=$(hdfs dfs -ls "$TRAINING_BASE" 2>/dev/null | grep "^d" | awk '{print $NF}' | xargs -n1 basename | sort | awk -v last="$LAST_PROCESSED" '$0 > last' | wc -l)
    PENDING_TEST=$(hdfs dfs -ls "$TESTING_BASE" 2>/dev/null | grep "^d" | awk '{print $NF}' | xargs -n1 basename | sort | awk -v last="$LAST_PROCESSED" '$0 > last' | wc -l)
    
    echo "Pending partitions:"
    echo "  Training: $PENDING_TRAIN"
    echo "  Testing:  $PENDING_TEST"
else
    echo "No MapReduce runs yet"
fi

# Recent MapReduce logs
RECENT_LOGS=$(ls -t "$SCRIPT_DIR/logs"/mapreduce_*.log 2>/dev/null | head -3)
if [ ! -z "$RECENT_LOGS" ]; then
    echo ""
    echo "Recent runs:"
    for log in $RECENT_LOGS; do
        BASENAME=$(basename "$log")
        RUN_ID=${BASENAME#mapreduce_}
        RUN_ID=${RUN_ID%.log}
        
        ACCURACY=$(grep "Accuracy:" "$log" | tail -1 | awk '{print $2}')
        if [ ! -z "$ACCURACY" ]; then
            echo "  $RUN_ID - Accuracy: $ACCURACY"
        else
            echo "  $RUN_ID - (in progress or failed)"
        fi
    done
fi

echo ""

# 4. MongoDB Statistics
echo -e "${CYAN}[4] MongoDB Statistics${NC}"
echo "----------------------------------------"

if command -v mongosh &> /dev/null; then
    MONGO_AVAILABLE=$(mongosh --quiet --eval "db.version()" 2>/dev/null)
    if [ $? -eq 0 ]; then
        PRED_COUNT=$(mongosh --quiet sentiment_analysis --eval "db.hadoop_predictions.countDocuments()" 2>/dev/null)
        METRICS_COUNT=$(mongosh --quiet sentiment_analysis --eval "db.hadoop_batch_metrics.countDocuments()" 2>/dev/null)
        
        echo "  Predictions: $PRED_COUNT"
        echo "  Batch Metrics: $METRICS_COUNT"
        
        # Latest accuracy
        LATEST_ACCURACY=$(mongosh --quiet sentiment_analysis --eval "db.hadoop_batch_metrics.find().sort({timestamp:-1}).limit(1).forEach(doc => print(doc.accuracy))" 2>/dev/null)
        if [ ! -z "$LATEST_ACCURACY" ]; then
            echo "  Latest Accuracy: $LATEST_ACCURACY"
        fi
    else
        echo -e "${YELLOW}  MongoDB not accessible${NC}"
    fi
else
    echo -e "${YELLOW}  mongosh not installed${NC}"
fi

echo ""

# 5. System Resources
echo -e "${CYAN}[5] System Resources${NC}"
echo "----------------------------------------"

# Disk usage
HDFS_USED=$(hdfs dfs -df -h / 2>/dev/null | tail -1 | awk '{print $3}')
HDFS_AVAILABLE=$(hdfs dfs -df -h / 2>/dev/null | tail -1 | awk '{print $4}')
HDFS_PERCENT=$(hdfs dfs -df / 2>/dev/null | tail -1 | awk '{print $5}')

echo "HDFS:"
echo "  Used: $HDFS_USED"
echo "  Available: $HDFS_AVAILABLE"
echo "  Usage: $HDFS_PERCENT"

echo ""

# Kafka topics
echo "Kafka Topics:"
TRAIN_LAG=$(kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group hdfs-consumer-continuous-tweets-training --describe 2>/dev/null | tail -n +3 | awk '{sum+=$5} END {print sum}')
TEST_LAG=$(kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group hdfs-consumer-continuous-tweets-testing --describe 2>/dev/null | tail -n +3 | awk '{sum+=$5} END {print sum}')

if [ ! -z "$TRAIN_LAG" ]; then
    echo "  Training lag: $TRAIN_LAG messages"
else
    echo "  Training lag: N/A"
fi

if [ ! -z "$TEST_LAG" ]; then
    echo "  Testing lag: $TEST_LAG messages"
else
    echo "  Testing lag: N/A"
fi

echo ""
echo "=========================================="
echo "  Timestamp: $(date)"
echo "=========================================="
echo ""
echo "Commands:"
echo "  $SCRIPT_DIR/kafka_consumer_daemon.sh status  - Check consumer"
echo "  tail -f $CONSUMER_LOG                         - Follow logs"
echo "  $SCRIPT_DIR/scheduled_mapreduce.sh            - Run MapReduce"
echo ""
