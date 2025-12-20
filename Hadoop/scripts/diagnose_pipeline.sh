#!/bin/bash

# Quick Fix for MapReduce Failure
# Run this to diagnose and fix common issues

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo "=========================================="
echo "  Pipeline Diagnostic Tool"
echo "=========================================="
echo ""

# 1. Check HDFS data
echo -e "${CYAN}[1] Checking HDFS Data${NC}"
echo "----------------------------------------"

TRAIN_PARTITIONS=$(hdfs dfs -ls /user/hadoop/kafka_data/tweets-training/ 2>/dev/null | grep "^d" | wc -l)
TEST_PARTITIONS=$(hdfs dfs -ls /user/hadoop/kafka_data/tweets-testing/ 2>/dev/null | grep "^d" | wc -l)

echo "Training partitions: $TRAIN_PARTITIONS"
echo "Testing partitions:  $TEST_PARTITIONS"

if [ $TRAIN_PARTITIONS -eq 0 ]; then
    echo -e "${RED}✗ No training data in HDFS${NC}"
    echo ""
    echo "Recommendation: Start training consumer"
    echo "  ./kafka_consumer_daemon.sh start tweets-training /user/hadoop/kafka_data/tweets-training hourly"
fi

if [ $TEST_PARTITIONS -eq 0 ]; then
    echo -e "${RED}✗ No testing data in HDFS${NC}"
    echo ""
    echo "Recommendation: Start testing consumer"
    echo "  ./start_dual_consumers.sh"
    echo ""
    echo "OR manually start testing consumer:"
    echo "  ./kafka_consumer_daemon.sh start tweets-testing /user/hadoop/kafka_data/tweets-testing hourly"
fi

if [ $TRAIN_PARTITIONS -gt 0 ] && [ $TEST_PARTITIONS -gt 0 ]; then
    echo -e "${GREEN}✓ Both training and testing data available${NC}"
fi

echo ""

# 2. Check consumers
echo -e "${CYAN}[2] Checking Consumers${NC}"
echo "----------------------------------------"

if [ -f "$SCRIPT_DIR/kafka_consumer.pid" ]; then
    PID=$(cat "$SCRIPT_DIR/kafka_consumer.pid")
    if kill -0 $PID 2>/dev/null; then
        echo -e "${GREEN}✓ Training consumer running${NC} (PID: $PID)"
    else
        echo -e "${RED}✗ Training consumer not running${NC}"
    fi
else
    echo -e "${RED}✗ Training consumer not started${NC}"
fi

if [ -f "$SCRIPT_DIR/kafka_consumer_testing.pid" ]; then
    PID=$(cat "$SCRIPT_DIR/kafka_consumer_testing.pid")
    if kill -0 $PID 2>/dev/null; then
        echo -e "${GREEN}✓ Testing consumer running${NC} (PID: $PID)"
    else
        echo -e "${RED}✗ Testing consumer not running${NC}"
    fi
else
    echo -e "${RED}✗ Testing consumer not started${NC}"
fi

echo ""

# 3. Check Kafka topics
echo -e "${CYAN}[3] Checking Kafka Topics${NC}"
echo "----------------------------------------"

TRAIN_MSGS=$(kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic tweets-training 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')
TEST_MSGS=$(kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic tweets-testing 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')

if [ ! -z "$TRAIN_MSGS" ] && [ $TRAIN_MSGS -gt 0 ]; then
    echo -e "${GREEN}✓ Training topic has messages${NC}: $TRAIN_MSGS"
else
    echo -e "${YELLOW}⚠ Training topic empty or unavailable${NC}"
fi

if [ ! -z "$TEST_MSGS" ] && [ $TEST_MSGS -gt 0 ]; then
    echo -e "${GREEN}✓ Testing topic has messages${NC}: $TEST_MSGS"
else
    echo -e "${YELLOW}⚠ Testing topic empty or unavailable${NC}"
fi

echo ""

# 4. Check MapReduce state
echo -e "${CYAN}[4] Checking MapReduce State${NC}"
echo "----------------------------------------"

if [ -f "$SCRIPT_DIR/.mapreduce_state" ]; then
    LAST_PROCESSED=$(cat "$SCRIPT_DIR/.mapreduce_state")
    echo "Last processed: $LAST_PROCESSED"
    
    # Check if there's new data
    NEW_TRAIN=$(hdfs dfs -ls /user/hadoop/kafka_data/tweets-training/ 2>/dev/null | grep "^d" | awk '{print $NF}' | xargs -n1 basename | sort | awk -v last="$LAST_PROCESSED" '$0 > last' | wc -l)
    NEW_TEST=$(hdfs dfs -ls /user/hadoop/kafka_data/tweets-testing/ 2>/dev/null | grep "^d" | awk '{print $NF}' | xargs -n1 basename | sort | awk -v last="$LAST_PROCESSED" '$0 > last' | wc -l)
    
    echo "New training partitions: $NEW_TRAIN"
    echo "New testing partitions:  $NEW_TEST"
    
    if [ $NEW_TRAIN -gt 0 ] && [ $NEW_TEST -gt 0 ]; then
        echo -e "${GREEN}✓ New data available for processing${NC}"
    else
        echo -e "${YELLOW}⚠ Waiting for new data${NC}"
    fi
else
    echo "No previous runs (first time)"
    if [ $TRAIN_PARTITIONS -gt 0 ] && [ $TEST_PARTITIONS -gt 0 ]; then
        echo -e "${GREEN}✓ Ready for first MapReduce run${NC}"
    fi
fi

echo ""

# 5. Recommendations
echo -e "${CYAN}[5] Recommendations${NC}"
echo "----------------------------------------"

if [ $TRAIN_PARTITIONS -eq 0 ] || [ $TEST_PARTITIONS -eq 0 ]; then
    echo -e "${YELLOW}Action Required:${NC}"
    echo ""
    echo "1. Make sure Kafka producer is running:"
    echo "   cd ../kafka_producer && source venv/bin/activate"
    echo "   python3 tweet_producer.py --csv-file ../data/raws/training.csv --rate 100"
    echo ""
    echo "2. Start BOTH consumers:"
    echo "   ./start_dual_consumers.sh"
    echo ""
    echo "3. Wait 2-3 minutes for data to accumulate"
    echo ""
    echo "4. Run MapReduce:"
    echo "   ./scheduled_mapreduce.sh"
elif [ ! -f "$SCRIPT_DIR/kafka_consumer.pid" ] || [ ! -f "$SCRIPT_DIR/kafka_consumer_testing.pid" ]; then
    echo -e "${YELLOW}Action Required:${NC}"
    echo ""
    echo "Start both consumers:"
    echo "   ./start_dual_consumers.sh"
else
    echo -e "${GREEN}✓ Pipeline looks healthy!${NC}"
    echo ""
    echo "You can:"
    echo "  1. Monitor: ./monitor_pipeline.sh"
    echo "  2. Run MapReduce manually: ./scheduled_mapreduce.sh"
    echo "  3. Check logs: tail -f kafka_consumer*.log"
fi

echo ""
echo "=========================================="
