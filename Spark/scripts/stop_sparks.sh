#!/bin/bash

echo "========================================="
echo "Stopping Spark Streaming Jobs"
echo "========================================="
echo ""

# Function to gracefully stop a process
graceful_stop() {
    local PROCESS_NAME=$1
    local PID=$2
    
    echo "Stopping $PROCESS_NAME (PID: $PID)..."
    
    # Send SIGTERM for graceful shutdown
    kill -TERM $PID 2>/dev/null
    
    # Wait up to 60 seconds
    for i in {1..60}; do
        if ! ps -p $PID > /dev/null 2>&1; then
            echo "✓ $PROCESS_NAME stopped gracefully"
            return 0
        fi
        sleep 1
        if [ $((i % 10)) -eq 0 ]; then
            echo "  Still waiting... ($i seconds)"
        fi
    done
    
    # Force kill if still running
    if ps -p $PID > /dev/null 2>&1; then
        echo "⚠ Timeout reached. Force stopping $PROCESS_NAME..."
        kill -9 $PID 2>/dev/null
        sleep 2
        
        if ! ps -p $PID > /dev/null 2>&1; then
            echo "✓ $PROCESS_NAME force stopped"
            return 0
        else
            echo "✗ Failed to stop $PROCESS_NAME"
            return 1
        fi
    fi
}

# Stop NB Streaming
NB_PID=$(pgrep -f "spark-submit.*NB_Streaming")
if [ ! -z "$NB_PID" ]; then
    graceful_stop "NB Streaming" $NB_PID
else
    echo "NB Streaming is not running"
fi

echo ""

# Stop SVM Streaming
SVM_PID=$(pgrep -f "spark-submit.*SVM_Streaming")
if [ ! -z "$SVM_PID" ]; then
    graceful_stop "SVM Streaming" $SVM_PID
else
    echo "SVM Streaming is not running"
fi

echo ""

# Check if any Spark processes are still running
REMAINING=$(pgrep -f "spark-submit" | wc -l)
if [ $REMAINING -eq 0 ]; then
    echo "========================================="
    echo "✓ All Spark Streaming jobs stopped"
    echo "========================================="
else
    echo "========================================="
    echo "⚠ Warning: $REMAINING Spark processes still running"
    echo "========================================="
    echo ""
    echo "Running Spark processes:"
    ps aux | grep spark-submit | grep -v grep
fi
