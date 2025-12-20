#!/bin/bash

# Compile Kafka to HDFS Consumer

echo "========================================="
echo "Compiling KafkaToHDFSConsumer.java"
echo "========================================="

# Set classpath
export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*

# Download Kafka client library if not present
KAFKA_DIR="/usr/local/kafka/libs"
DEPS_DIR="./lib"
mkdir -p $DEPS_DIR

echo "Checking dependencies..."

# Kafka clients
KAFKA_CLIENT_JAR="$DEPS_DIR/kafka-clients-3.6.1.jar"
if [ ! -f "$KAFKA_CLIENT_JAR" ]; then
    if [ -f "$KAFKA_DIR/kafka-clients-3.6.1.jar" ]; then
        echo "Copying Kafka client from Kafka installation..."
        cp "$KAFKA_DIR/kafka-clients-3.6.1.jar" "$DEPS_DIR/"
    else
        echo "Downloading Kafka client library..."
        wget -O "$KAFKA_CLIENT_JAR" https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar
    fi
fi

# SLF4J (required by Kafka client)
SLF4J_API_JAR="$DEPS_DIR/slf4j-api-1.7.36.jar"
if [ ! -f "$SLF4J_API_JAR" ]; then
    if [ -f "$KAFKA_DIR/slf4j-api-1.7.36.jar" ]; then
        cp "$KAFKA_DIR/slf4j-api-1.7.36.jar" "$DEPS_DIR/"
    else
        echo "Downloading SLF4J API..."
        wget -O "$SLF4J_API_JAR" https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
    fi
fi

# SLF4J Simple (for logging)
SLF4J_SIMPLE_JAR="$DEPS_DIR/slf4j-simple-1.7.36.jar"
if [ ! -f "$SLF4J_SIMPLE_JAR" ]; then
    echo "Downloading SLF4J Simple..."
    wget -O "$SLF4J_SIMPLE_JAR" https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar
fi

echo "✓ All dependencies ready"
echo ""

# Compile
echo "Compiling KafkaToHDFSConsumer.java..."
javac -classpath "$HADOOP_CLASSPATH:$DEPS_DIR/*" -d . KafkaToHDFSConsumer.java

echo "Compiling KafkaToHDFSConsumerContinuous.java..."
javac -classpath "$HADOOP_CLASSPATH:$DEPS_DIR/*" -d . KafkaToHDFSConsumerContinuous.java

if [ $? -eq 0 ]; then
    echo "✓ Compilation successful"
    
    # Create JAR
    echo ""
    echo "Creating JAR file..."
    jar -cvf kafka_to_hdfs_consumer.jar KafkaToHDFSConsumer*.class
    
    echo ""
    echo "========================================="
    echo "✓ Build Complete!"
    echo "========================================="
    echo "Output: kafka_to_hdfs_consumer.jar"
    echo ""
    echo "To run:"
    echo "  hadoop jar kafka_to_hdfs_consumer.jar KafkaToHDFSConsumer \\"
    echo "    tweets-training \\"
    echo "    /user/hadoop/kafka_data/tweets-training \\"
    echo "    5000"
    echo "========================================="
else
    echo "✗ Compilation failed"
    exit 1
fi
