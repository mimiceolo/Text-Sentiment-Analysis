#!/bin/bash

# Compile KafkaHadoopNB.java with MongoDB and JSON dependencies

echo "========================================="
echo "Compiling KafkaHadoopNB.java"
echo "========================================="

# Set HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*

# Download dependencies if not present
DEPS_DIR="./lib"
mkdir -p $DEPS_DIR

echo "Checking dependencies..."

# MongoDB Java Driver
MONGO_JAR="$DEPS_DIR/mongodb-driver-sync-4.11.1.jar"
if [ ! -f "$MONGO_JAR" ]; then
    echo "Downloading MongoDB Java Driver..."
    wget -O $MONGO_JAR https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar
fi

# MongoDB BSON
BSON_JAR="$DEPS_DIR/bson-4.11.1.jar"
if [ ! -f "$BSON_JAR" ]; then
    echo "Downloading BSON library..."
    wget -O $BSON_JAR https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar
fi

# MongoDB Core
MONGO_CORE_JAR="$DEPS_DIR/mongodb-driver-core-4.11.1.jar"
if [ ! -f "$MONGO_CORE_JAR" ]; then
    echo "Downloading MongoDB Core Driver..."
    wget -O $MONGO_CORE_JAR https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar
fi

# JSON library
JSON_JAR="$DEPS_DIR/json-20230227.jar"
if [ ! -f "$JSON_JAR" ]; then
    echo "Downloading JSON library..."
    wget -O $JSON_JAR https://repo1.maven.org/maven2/org/json/json/20230227/json-20230227.jar
fi

echo "✓ All dependencies ready"
echo ""

# Compile
echo "Compiling TrainModel.java..."
javac -classpath "$HADOOP_CLASSPATH:$DEPS_DIR/*" -d . TrainModel.java

if [ $? -ne 0 ]; then
    echo "✗ TrainModel compilation failed"
    exit 1
fi

echo "Compiling PredictSentiment.java..."
javac -classpath "$HADOOP_CLASSPATH:$DEPS_DIR/*" -d . PredictSentiment.java

if [ $? -eq 0 ]; then
    echo "✓ Compilation successful"
    
    # Create JAR
    echo ""
    echo "Creating JAR file..."
    jar -cvf kafka_hadoop_nb.jar -C . *.class
    
    # Add dependencies to JAR
    echo ""
    echo "Adding dependencies to JAR..."
    cd $DEPS_DIR
    for jar in *.jar; do
        jar -xf $jar
    done
    cd ..
    jar -uvf kafka_hadoop_nb.jar -C $DEPS_DIR .
    
    echo ""
    echo "========================================="
    echo "✓ Build Complete!"
    echo "========================================="
    echo "Output: kafka_hadoop_nb.jar"
    echo ""
    echo "Available classes:"
    echo "  1. TrainModel        - Train model ONCE and save to HDFS"
    echo "  2. PredictSentiment  - Load model and predict (real-time)"
    echo ""
    echo "Recommended workflow:"
    echo ""
    echo "  # Step 1: Train model once"
    echo "  hadoop jar kafka_hadoop_nb.jar TrainModel \\"
    echo "    /user/hadoop/kafka_data/tweets-training \\"
    echo "    /user/hadoop/models/nb_model"
    echo ""
    echo "  # Step 2: Predict on streaming data (run repeatedly)"
    echo "  hadoop jar kafka_hadoop_nb.jar PredictSentiment \\"
    echo "    /user/hadoop/models/nb_model \\"
    echo "    /user/hadoop/kafka_data/tweets-testing/2025-12-20-18 \\"
    echo "    /user/hadoop/predictions/batch_20251220_18"
    echo ""
    echo "========================================="
else
    echo "✗ Compilation failed"
    exit 1
fi
