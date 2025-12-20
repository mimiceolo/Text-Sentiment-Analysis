# Spark Project Compilation Guide

## Overview

This Spark project uses **SBT (Scala Build Tool)** to compile Scala sources and create a fat JAR with all dependencies.

## Project Structure

```
Spark/
├── build.sbt                    # SBT build configuration
├── project/
│   ├── assembly.sbt             # Plugin for creating fat JARs
│   └── build.properties         # SBT version
├── src/
│   └── main/
│       └── scala/
│           ├── NB.scala                      # Naive Bayes batch training
│           ├── NB_Streaming.scala            # NB streaming (console output)
│           ├── NB_Streaming_MongoDB.scala    # NB streaming (MongoDB output)
│           ├── SVM.scala                     # SVM batch training
│           ├── SVM_Streaming.scala           # SVM streaming (console output)
│           ├── SVM_Streaming_MongoDB.scala   # SVM streaming (MongoDB output)
│           └── TrainModels.scala             # Train all models
└── target/                      # Compiled output (generated)
```

## Prerequisites

### 1. Install SBT

**Ubuntu/Debian:**
```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```

**Verify installation:**
```bash
sbt --version
```

### 2. Ensure Spark is Installed

```bash
spark-submit --version
```

## Compilation Methods

### Method 1: Using the Compile Script (Recommended)

```bash
cd Spark
./compile.sh
```

This will:
1. ✓ Check SBT installation
2. ✓ List all source files
3. ✓ Clean previous builds
4. ✓ Compile Scala sources
5. ✓ Create fat JAR with dependencies
6. ✓ Show available main classes and usage examples

### Method 2: Manual SBT Commands

```bash
cd Spark

# Clean previous builds
sbt clean

# Compile only (no JAR)
sbt compile

# Create fat JAR with dependencies
sbt assembly

# Run tests (if available)
sbt test
```

### Method 3: Interactive SBT Shell

```bash
cd Spark
sbt

# Inside SBT shell:
sbt> clean
sbt> compile
sbt> assembly
sbt> exit
```

## Build Output

After successful compilation:

```
target/scala-2.12/sentiment-analysis-streaming-assembly.jar
```

**Typical JAR size:** ~15-30 MB (includes Kafka, MongoDB drivers)

## Dependencies

The project includes:

- **Spark Core** 3.4.1 (provided by spark-submit)
- **Spark SQL** 3.4.1 (provided)
- **Spark MLlib** 3.4.1 (provided)
- **Spark Streaming** 3.4.1 (provided)
- **Spark Streaming Kafka** 3.4.1 (bundled)
- **Kafka Clients** 3.6.1 (bundled)
- **MongoDB Driver** 4.11.1 (bundled)

## Usage Examples

### 1. Train Naive Bayes Model (Batch)

```bash
spark-submit \
  --class NB \
  --master local[*] \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  hdfs://localhost:9000/user/hadoop/training_data \
  hdfs://localhost:9000/user/hadoop/test_data \
  hdfs://localhost:9000/user/hadoop/spark_models/nb_model
```

**Arguments:**
1. Training data path (HDFS)
2. Test data path (HDFS)
3. Model output path (HDFS)

### 2. Train SVM Model (Batch)

```bash
spark-submit \
  --class SVM \
  --master local[*] \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  hdfs://localhost:9000/user/hadoop/training_data \
  hdfs://localhost:9000/user/hadoop/test_data \
  hdfs://localhost:9000/user/hadoop/spark_models/svm_model
```

### 3. Real-time NB Streaming (Console Output)

```bash
spark-submit \
  --class NB_Streaming \
  --master local[*] \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  localhost:9092 \
  tweets-testing \
  hdfs://localhost:9000/user/hadoop/spark_models/nb_model
```

**Arguments:**
1. Kafka broker (localhost:9092)
2. Kafka topic (tweets-testing)
3. Model path (HDFS)

### 4. Real-time NB Streaming (MongoDB Output)

```bash
spark-submit \
  --class NB_Streaming_MongoDB \
  --master local[*] \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  localhost:9092 \
  tweets-testing \
  sentiment_analysis \
  hdfs://localhost:9000/user/hadoop/spark_models/nb_model
```

**Arguments:**
1. Kafka broker (localhost:9092)
2. Kafka topic (tweets-testing)
3. MongoDB database (sentiment_analysis)
4. Model path (HDFS)

**MongoDB Collections:**
- `predictions` - Individual tweet predictions
- `batch_metrics` - Batch-level metrics (accuracy, F1, etc.)

### 5. Train All Models

```bash
spark-submit \
  --class TrainModels \
  --master local[*] \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  hdfs://localhost:9000/user/hadoop/training_data \
  hdfs://localhost:9000/user/hadoop/test_data \
  hdfs://localhost:9000/user/hadoop/spark_models
```

## Common Issues & Solutions

### Issue 1: SBT Not Found

**Error:** `sbt: command not found`

**Solution:** Install SBT following the prerequisites section above.

### Issue 2: Out of Memory During Compilation

**Error:** `java.lang.OutOfMemoryError: Java heap space`

**Solution:** Increase SBT memory:
```bash
export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled"
sbt assembly
```

### Issue 3: Assembly Merge Conflicts

**Error:** `deduplicate: different file contents found`

**Solution:** The `build.sbt` already includes merge strategies. If issues persist, check the `assemblyMergeStrategy` section in `build.sbt`.

### Issue 4: Kafka Dependencies Missing at Runtime

**Error:** `ClassNotFoundException: org.apache.kafka...`

**Solution:** The JAR should include Kafka dependencies. Verify:
```bash
jar tf target/scala-2.12/sentiment-analysis-streaming-assembly.jar | grep kafka
```

### Issue 5: Spark Version Mismatch

**Error:** `Version mismatch` or incompatibility errors

**Solution:** Ensure your Spark installation matches the version in `build.sbt` (3.4.1):
```bash
spark-submit --version
```

## Development Workflow

### 1. Make Code Changes
Edit files in `src/main/scala/`

### 2. Compile & Test Locally
```bash
./compile.sh
```

### 3. Test with spark-submit
```bash
spark-submit --class <YourClass> \
  --master local[*] \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  <args...>
```

### 4. Monitor Spark UI
Open `http://localhost:4040` while Spark job is running

## Performance Tuning

### Spark Configuration

```bash
spark-submit \
  --class NB_Streaming_MongoDB \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.streaming.kafka.maxRatePerPartition=1000 \
  --conf spark.sql.shuffle.partitions=4 \
  target/scala-2.12/sentiment-analysis-streaming-assembly.jar \
  <args...>
```

### SBT Build Performance

Add to `~/.sbt/1.0/global.sbt`:
```scala
// Parallel execution
parallelExecution := true

// Use more cores
concurrentRestrictions in Global := Seq(
  Tags.limitAll(8)
)
```

## Clean Build

To completely clean and rebuild:

```bash
cd Spark
rm -rf target project/target
sbt clean
sbt compile
sbt assembly
```

## Verify JAR Contents

```bash
# List all classes
jar tf target/scala-2.12/sentiment-analysis-streaming-assembly.jar | grep "\.class$"

# Check main classes
jar tf target/scala-2.12/sentiment-analysis-streaming-assembly.jar | grep -E "NB\.class|SVM\.class|TrainModels\.class"

# Check dependencies
jar tf target/scala-2.12/sentiment-analysis-streaming-assembly.jar | grep -E "kafka|mongodb"
```

## Build Information

- **Scala Version:** 2.12.18
- **Spark Version:** 3.4.1
- **SBT Version:** (defined in `project/build.properties`)
- **Build Tool:** SBT with sbt-assembly plugin
- **Output:** Fat JAR with all dependencies

## Quick Reference

| Command | Description |
|---------|-------------|
| `./compile.sh` | Compile and create JAR |
| `sbt clean` | Remove all build artifacts |
| `sbt compile` | Compile sources only |
| `sbt assembly` | Create fat JAR |
| `sbt test` | Run tests |
| `sbt console` | Interactive Scala REPL |
| `sbt ~compile` | Auto-recompile on file changes |

## Next Steps

1. ✅ Compile the project: `./compile.sh`
2. ✅ Train models (batch): Use `NB` or `SVM` classes
3. ✅ Start streaming: Use `*_Streaming_MongoDB` classes
4. ✅ Monitor results: Check MongoDB and Spark UI
5. ✅ View dashboard: `http://localhost:5000/dashboard`

---

**📝 Note:** The fat JAR includes all dependencies except Spark itself (which is provided by `spark-submit`).
