#!/bin/bash

# Spark Sentiment Analysis - Compilation Script
# Compiles Scala files and creates fat JAR with dependencies
cd ..

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================="
echo -e "${BLUE}Compiling Spark Sentiment Analysis${NC}"
echo "========================================="
echo ""

# Check if SBT is installed
if ! command -v sbt &> /dev/null; then
    echo -e "${RED}✗ SBT not found!${NC}"
    echo ""
    echo "Install SBT:"
    echo "  sudo apt-get update"
    echo "  sudo apt-get install sbt"
    echo ""
    exit 1
fi

SBT_VERSION=$(sbt --version 2>&1 | grep -i "sbt version" | awk '{print $NF}')
echo -e "${GREEN}✓${NC} SBT found: version $SBT_VERSION"
echo ""

# List source files
echo "Source files:"
if [ -d "src/main/scala" ]; then
    find src/main/scala -name "*.scala" | sort | sed 's|^|  - |'
    FILE_COUNT=$(find src/main/scala -name "*.scala" | wc -l)
    echo "  Total: $FILE_COUNT files"
else
    echo -e "${RED}  ✗ No src/main/scala directory found${NC}"
    exit 1
fi
echo ""

# Clean previous builds
echo -e "${YELLOW}[1/3] Cleaning previous builds...${NC}"
sbt clean

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}✗ Clean failed!${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}[2/3] Compiling Scala sources...${NC}"
sbt compile

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}✗ Compilation failed!${NC}"
    echo ""
    echo "Common issues:"
    echo "  - Check Scala syntax errors"
    echo "  - Verify all imports are correct"
    echo "  - Ensure dependencies in build.sbt are accessible"
    exit 1
fi

echo ""
echo -e "${YELLOW}[3/3] Creating fat JAR with dependencies...${NC}"
sbt assembly

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}✗ Assembly failed!${NC}"
    echo ""
    echo "Common issues:"
    echo "  - Dependency conflicts (check build.sbt)"
    echo "  - Out of memory (try: export SBT_OPTS='-Xmx2G')"
    exit 1
fi

echo ""
echo "========================================="
echo -e "${GREEN}✓ Build Complete!${NC}"
echo "========================================="

# Find the generated JAR
JAR_FILE=$(find target -name "*-assembly.jar" 2>/dev/null | head -1)

if [ -n "$JAR_FILE" ] && [ -f "$JAR_FILE" ]; then
    JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
    echo ""
    echo -e "Output JAR: ${GREEN}$JAR_FILE${NC}"
    echo "Size:       $JAR_SIZE"
    echo ""
    
    echo "Available Classes:"
    echo "  1. TrainModels            - Train NB and SVM models (batch)"
    echo "  2. NB_Streaming_MongoDB   - Naive Bayes streaming → MongoDB"
    echo "  3. SVM_Streaming_MongoDB  - SVM streaming → MongoDB"
    echo ""
    
    echo "========================================="
    echo "Usage Examples"
    echo "========================================="
    echo ""
    
    echo -e "${BLUE}1. Train Models (NB + SVM):${NC}"
    echo "spark-submit \\"
    echo "  --class TrainModels \\"
    echo "  --master local[*] \\"
    echo "  $JAR_FILE \\"
    echo "  hdfs://localhost:9000/user/hadoop/kafka_data/tweets-training \\"
    echo "  hdfs://localhost:9000/user/hadoop/kafka_data/tweets-testing \\"
    echo "  hdfs://localhost:9000/user/hadoop/spark_models"
    echo ""
    
    echo -e "${BLUE}2. NB Real-time Streaming (MongoDB):${NC}"
    echo "spark-submit \\"
    echo "  --class NB_Streaming_MongoDB \\"
    echo "  --master local[*] \\"
    echo "  $JAR_FILE \\"
    echo "  localhost:9092 \\"
    echo "  tweets-testing \\"
    echo "  sentiment_analysis \\"
    echo "  hdfs://localhost:9000/user/hadoop/spark_models/nb_model"
    echo ""
    
    echo -e "${BLUE}3. SVM Real-time Streaming (MongoDB):${NC}"
    echo "spark-submit \\"
    echo "  --class SVM_Streaming_MongoDB \\"
    echo "  --master local[*] \\"
    echo "  $JAR_FILE \\"
    echo "  localhost:9092 \\"
    echo "  tweets-testing \\"
    echo "  sentiment_analysis \\"
    echo "  hdfs://localhost:9000/user/hadoop/spark_models/svm_model"
    echo ""
    
    echo "========================================="
    echo -e "${GREEN}Ready to submit Spark jobs!${NC}"
    echo "========================================="
else
    echo ""
    echo -e "${RED}✗ JAR file not found!${NC}"
    echo "Expected location: target/scala-2.12/*-assembly.jar"
    exit 1
fi
