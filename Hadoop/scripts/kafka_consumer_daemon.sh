#!/bin/bash

# Continuous Kafka to HDFS Consumer Daemon
# Runs in background, tracks offsets, uses time-based partitioning

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/kafka_consumer.pid"
LOG_FILE="$SCRIPT_DIR/kafka_consumer.log"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

start_consumer() {
    local TOPIC=$1
    local HDFS_DIR=$2
    local PARTITION_MODE=$3
    
    if [ -z "$TOPIC" ] || [ -z "$HDFS_DIR" ] || [ -z "$PARTITION_MODE" ]; then
        echo -e "${RED}Error: Missing arguments${NC}"
        echo "Usage: $0 start <topic> <hdfs_dir> <partition_mode>"
        echo "  partition_mode: hourly or minutely"
        exit 1
    fi
    
    # Check if already running
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo -e "${YELLOW}Consumer is already running (PID: $PID)${NC}"
            exit 1
        else
            echo "Removing stale PID file..."
            rm -f "$PID_FILE"
        fi
    fi
    
    echo "=========================================="
    echo "Starting Continuous Kafka Consumer"
    echo "=========================================="
    echo "Topic:          $TOPIC"
    echo "HDFS Directory: $HDFS_DIR"
    echo "Partition Mode: $PARTITION_MODE"
    echo "Log File:       $LOG_FILE"
    echo "=========================================="
    
    # Set classpath
    export HADOOP_CLASSPATH="$SCRIPT_DIR/lib/*"
    
    # Start consumer in background
    nohup hadoop jar "$SCRIPT_DIR/kafka_to_hdfs_consumer.jar" \
        KafkaToHDFSConsumerContinuous \
        "$TOPIC" \
        "$HDFS_DIR" \
        "$PARTITION_MODE" \
        >> "$LOG_FILE" 2>&1 &
    
    PID=$!
    echo $PID > "$PID_FILE"
    
    sleep 2
    
    if kill -0 $PID 2>/dev/null; then
        echo -e "${GREEN}✓ Consumer started (PID: $PID)${NC}"
        echo ""
        echo "Monitor logs:"
        echo "  tail -f $LOG_FILE"
        echo ""
        echo "Stop consumer:"
        echo "  $0 stop"
    else
        echo -e "${RED}✗ Failed to start consumer${NC}"
        rm -f "$PID_FILE"
        exit 1
    fi
}

stop_consumer() {
    if [ ! -f "$PID_FILE" ]; then
        echo -e "${YELLOW}Consumer is not running${NC}"
        exit 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ! kill -0 $PID 2>/dev/null; then
        echo -e "${YELLOW}Consumer is not running (stale PID file)${NC}"
        rm -f "$PID_FILE"
        exit 1
    fi
    
    echo "Stopping consumer (PID: $PID)..."
    kill -TERM $PID
    
    # Wait for graceful shutdown (max 30 seconds)
    for i in {1..30}; do
        if ! kill -0 $PID 2>/dev/null; then
            echo -e "${GREEN}✓ Consumer stopped${NC}"
            rm -f "$PID_FILE"
            return 0
        fi
        sleep 1
    done
    
    # Force kill if still running
    echo "Forcing shutdown..."
    kill -9 $PID 2>/dev/null
    rm -f "$PID_FILE"
    echo -e "${GREEN}✓ Consumer forcefully stopped${NC}"
}

status_consumer() {
    if [ ! -f "$PID_FILE" ]; then
        echo -e "${RED}✗ Consumer is not running${NC}"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if kill -0 $PID 2>/dev/null; then
        echo -e "${GREEN}✓ Consumer is running (PID: $PID)${NC}"
        echo ""
        echo "Process info:"
        ps -p $PID -o pid,ppid,cmd,%mem,%cpu,etime
        echo ""
        echo "Recent log entries:"
        tail -20 "$LOG_FILE"
        return 0
    else
        echo -e "${RED}✗ Consumer is not running (stale PID file)${NC}"
        rm -f "$PID_FILE"
        return 1
    fi
}

restart_consumer() {
    echo "Restarting consumer..."
    stop_consumer 2>/dev/null
    sleep 2
    # Get parameters from command line
    start_consumer "$@"
}

case "${1:-}" in
    start)
        start_consumer "$2" "$3" "$4"
        ;;
    stop)
        stop_consumer
        ;;
    status)
        status_consumer
        ;;
    restart)
        restart_consumer "$2" "$3" "$4"
        ;;
    logs)
        if [ -f "$LOG_FILE" ]; then
            tail -f "$LOG_FILE"
        else
            echo "Log file not found: $LOG_FILE"
        fi
        ;;
    *)
        echo "Continuous Kafka to HDFS Consumer Daemon"
        echo ""
        echo "Usage: $0 {start|stop|status|restart|logs} [args...]"
        echo ""
        echo "Commands:"
        echo "  start <topic> <hdfs_dir> <partition_mode>  Start consumer daemon"
        echo "  stop                                        Stop consumer daemon"
        echo "  status                                      Check consumer status"
        echo "  restart <topic> <hdfs_dir> <partition_mode> Restart consumer"
        echo "  logs                                        Follow consumer logs"
        echo ""
        echo "Examples:"
        echo "  $0 start tweets-training /user/hadoop/kafka_data/tweets-training hourly"
        echo "  $0 status"
        echo "  $0 stop"
        echo ""
        echo "Partition modes:"
        echo "  hourly   - Create directory per hour (YYYY-MM-DD-HH)"
        echo "  minutely - Create directory per minute (YYYY-MM-DD-HH-mm)"
        exit 1
        ;;
esac
