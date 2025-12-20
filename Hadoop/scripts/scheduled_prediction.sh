#!/bin/bash

# Scheduled Prediction Runner (Real-time Streaming)
# Loads pre-trained model and predicts on new streaming data

# Set up Hadoop environment for cron jobs
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=/usr/local/hadoop
fi

# Set JAVA_HOME if not set
if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi

# Add Hadoop binaries to PATH
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="$SCRIPT_DIR/.prediction_state"
LOG_DIR="$SCRIPT_DIR/logs"
LOCK_FILE="$SCRIPT_DIR/.prediction.lock"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
MODEL_PATH="/user/hadoop/models/nb_model"
TESTING_BASE="/user/hadoop/kafka_data/tweets-testing"
PREDICTIONS_BASE="/user/hadoop/predictions"

# Create log directory
mkdir -p "$LOG_DIR"

# Function to acquire lock
acquire_lock() {
    if [ -f "$LOCK_FILE" ]; then
        LOCK_PID=$(cat "$LOCK_FILE")
        if kill -0 $LOCK_PID 2>/dev/null; then
            echo -e "${YELLOW}Another prediction job is running (PID: $LOCK_PID)${NC}"
            return 1
        else
            rm -f "$LOCK_FILE"
        fi
    fi
    echo $$ > "$LOCK_FILE"
    return 0
}

# Function to release lock
release_lock() {
    rm -f "$LOCK_FILE"
}

# Function to get last processed timestamp
get_last_processed() {
    if [ -f "$STATE_FILE" ]; then
        cat "$STATE_FILE"
    else
        echo "1970-01-01-00-00"
    fi
}

# Function to save last processed timestamp
save_last_processed() {
    local TIMESTAMP=$1
    echo "$TIMESTAMP" > "$STATE_FILE"
}

# Function to list new partitions
list_new_partitions() {
    local BASE_DIR=$1
    local LAST_PROCESSED=$2
    
    hdfs dfs -ls "$BASE_DIR" 2>/dev/null | \
        grep "^d" | \
        awk '{print $NF}' | \
        xargs -n1 basename | \
        sort | \
        awk -v last="$LAST_PROCESSED" '$0 > last'
}

# Function to run prediction
run_prediction() {
    local TESTING_PARTITIONS=$1
    local RUN_ID=$(date +%Y%m%d-%H%M%S)
    local LOG_FILE="$LOG_DIR/prediction_$RUN_ID.log"
    
    # Validate inputs
    if [ -z "$TESTING_PARTITIONS" ]; then
        echo -e "${RED}✗ Error: No testing partitions provided${NC}"
        return 1
    fi
    
    echo "=========================================="  | tee -a "$LOG_FILE"
    echo "Prediction Job: $RUN_ID"                     | tee -a "$LOG_FILE"
    echo "=========================================="  | tee -a "$LOG_FILE"
    echo "Model:               $MODEL_PATH"            | tee -a "$LOG_FILE"
    echo "Testing Partitions:  $TESTING_PARTITIONS"   | tee -a "$LOG_FILE"
    echo "Start Time: $(date)"                         | tee -a "$LOG_FILE"
    echo "=========================================="  | tee -a "$LOG_FILE"
    
    # Create input path list
    TESTING_PATHS=""
    for partition in $TESTING_PARTITIONS; do
        TESTING_PATHS="$TESTING_PATHS,$TESTING_BASE/$partition"
    done
    TESTING_PATHS=${TESTING_PATHS:1}
    
    # Create output directory for this run
    PREDICTIONS_OUTPUT="$PREDICTIONS_BASE/predictions_$RUN_ID"
    
    echo ""                                           | tee -a "$LOG_FILE"
    echo "Running Prediction..."                      | tee -a "$LOG_FILE"
    
    # Set classpath
    export HADOOP_CLASSPATH="$SCRIPT_DIR/lib/*"
    
    # Run Hadoop prediction job
    hadoop jar "$SCRIPT_DIR/kafka_hadoop_nb.jar" PredictSentiment \
        "$MODEL_PATH" \
        "$TESTING_PATHS" \
        "$PREDICTIONS_OUTPUT" \
        >> "$LOG_FILE" 2>&1
    
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo ""                                       | tee -a "$LOG_FILE"
        echo -e "${GREEN}✓ Prediction job completed${NC}" | tee -a "$LOG_FILE"
        
        echo "Predictions saved: $PREDICTIONS_OUTPUT"  | tee -a "$LOG_FILE"
        
        # Extract metrics from log
        ACCURACY=$(grep "Accuracy:" "$LOG_FILE" | tail -1 | awk '{print $2}')
        F1_SCORE=$(grep "F1-Score:" "$LOG_FILE" | tail -1 | awk '{print $2}')
        TWEETS=$(grep "Tweets processed:" "$LOG_FILE" | tail -1 | awk '{print $3}')
        
        echo ""                                       | tee -a "$LOG_FILE"
        echo "Tweets:   $TWEETS"                      | tee -a "$LOG_FILE"
        echo "Accuracy: $ACCURACY"                    | tee -a "$LOG_FILE"
        echo "F1-Score: $F1_SCORE"                    | tee -a "$LOG_FILE"
        
    else
        echo ""                                       | tee -a "$LOG_FILE"
        echo -e "${RED}✗ Prediction job failed (exit code: $EXIT_CODE)${NC}" | tee -a "$LOG_FILE"
    fi
    
    echo ""                                           | tee -a "$LOG_FILE"
    echo "End Time: $(date)"                         | tee -a "$LOG_FILE"
    echo "Log File: $LOG_FILE"                       | tee -a "$LOG_FILE"
    echo "=========================================="  | tee -a "$LOG_FILE"
    
    return $EXIT_CODE
}

# Main execution
main() {
    # Acquire lock
    if ! acquire_lock; then
        exit 1
    fi
    
    trap release_lock EXIT
    
    echo "=========================================="
    echo "Scheduled Prediction Runner"
    echo "=========================================="
    echo "Timestamp:  $(date)"
    echo "Model:      $MODEL_PATH"
    echo "=========================================="
    echo ""
    
    # Check if model exists
    if ! hdfs dfs -test -d "$MODEL_PATH" 2>/dev/null; then
        echo -e "${RED}✗ Model not found: $MODEL_PATH${NC}"
        echo ""
        echo "Train a model first:"
        echo "  hadoop jar kafka_hadoop_nb.jar TrainModel \\"
        echo "    /user/hadoop/kafka_data/tweets-training \\"
        echo "    $MODEL_PATH"
        exit 1
    fi
    
    echo -e "${GREEN}✓${NC} Model found: $MODEL_PATH"
    echo ""
    
    # Get last processed partition
    LAST_PROCESSED=$(get_last_processed)
    echo "Last processed: $LAST_PROCESSED"
    echo ""
    
    # Find new partitions
    NEW_TEST=$(list_new_partitions "$TESTING_BASE" "$LAST_PROCESSED")
    
    if [ -z "$NEW_TEST" ]; then
        echo -e "${YELLOW}No new testing data to process${NC}"
        echo "Waiting for new partitions after: $LAST_PROCESSED"
        exit 0
    fi
    
    echo "New testing partitions:"
    echo "$NEW_TEST" | sed 's/^/  /'
    echo ""
    
    # Run prediction on new data
    run_prediction "$NEW_TEST"
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        # Update state to latest partition
        LATEST=$(echo "$NEW_TEST" | tail -1)
        save_last_processed "$LATEST"
        echo ""
        echo "State updated: $LATEST"
    fi
    
    exit $EXIT_CODE
}

# Run main function
main "$@"
