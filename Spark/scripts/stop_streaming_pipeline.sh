#!/bin/bash

echo "=========================================="
echo "Stopping Sentiment Analysis Streaming Pipeline"
echo "=========================================="
echo ""

# Stop Kafka
echo "1. Stopping Kafka Broker..."
kafka-server-stop.sh
sleep 3

# Stop Zookeeper
echo "2. Stopping Zookeeper..."
zookeeper-server-stop.sh
sleep 3

# Stop Hadoop
echo "3. Stopping Hadoop YARN..."
stop-yarn.sh
sleep 3

echo "4. Stopping Hadoop HDFS..."
stop-dfs.sh
sleep 3

# Verify services stopped
echo ""
echo "5. Verifying services stopped..."
echo "------------------------------"
jps
echo "------------------------------"
echo ""

echo "=========================================="
echo "All services stopped"
echo "=========================================="
