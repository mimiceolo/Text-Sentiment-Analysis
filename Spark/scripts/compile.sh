#!/bin/bash

# Spark Scala Project Compilation Script
# Compiles all Scala files and creates a fat JAR with dependencies
cd ..

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
    echo "Install SBT first:"
    echo "  Ubuntu/Debian:"
    echo "    echo 'deb https://repo.scala-sbt.org/scalasbt/debian all main' | sudo tee /etc/apt/sources.list.d/sbt.list"
    echo "    curl -sL 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823' | sudo apt-key add"
    echo "    sudo apt-get update"
    echo "    sudo apt-get install sbt"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} SBT found: $(sbt --version | head -1)"
echo ""

# List source files
echo "Source files:"
find src/main/scala -name "*.scala" | sed 's|^|  - |'
echo ""

# Clean previous builds
echo -e "${YELLOW}Cleaning previous builds...${NC}"
sbt clean

echo ""
echo -e "${YELLOW}Compiling Scala sources...${NC}"
sbt compile

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}✗ Compilation failed!${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}Creating fat JAR with dependencies...${NC}"
sbt assembly

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}✗ Assembly failed!${NC}"
    exit 1
fi

echo ""
echo "========================================="
echo -e "${GREEN}✓ Build Complete!${NC}"
echo "========================================="

# Find the generated JAR
JAR_FILE=$(find target -name "*-assembly.jar" | head -1)

if [ -n "$JAR_FILE" ]; then
    JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
    echo -e "Output JAR: ${GREEN}$JAR_FILE${NC}"
    echo "Size: $JAR_SIZE"
    echo ""
    
    # Show available main classes
    echo "Available main classes:"
    echo "  1. NB                        - Train Naive Bayes model (batch)"
    echo "  2. SVM                       - Train SVM model (batch)"
    echo "  3. TrainModels               - Train all models"
    echo "  4. NB_Streaming              - Naive Bayes real-time streaming"
    echo "  5. NB_Streaming_MongoDB      - NB streaming with MongoDB output"
    echo "  6. SVM_Streaming             - SVM real-time streaming"
    echo "  7. SVM_Streaming_MongoDB     - SVM streaming with MongoDB output"
    echo ""
    
    echo "Example usage:"
    echo ""
    echo "# Train models (batch):"
    echo "spark-submit --class NB \\"
    echo "  --master local[*] \\"
    echo "  $JAR_FILE \\"
    echo "  hdfs://localhost:9000/user/hadoop/training_data \\"
    echo "  hdfs://localhost:9000/user/hadoop/test_data \\"
    echo "  hdfs://localhost:9000/user/hadoop/spark_models/nb_model"
    echo ""
    echo "# Real-time streaming with MongoDB:"
    echo "spark-submit --class NB_Streaming_MongoDB \\"
    echo "  --master local[*] \\"
    echo "  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 \\"
    echo "  $JAR_FILE \\"
    echo "  localhost:9092 \\"
    echo "  tweets-testing \\"
    echo "  sentiment_analysis \\"
    echo "  hdfs://localhost:9000/user/hadoop/spark_models/nb_model"
    echo ""
else
    echo -e "${RED}✗ JAR file not found in target directory${NC}"
    exit 1
fi

echo "========================================="
