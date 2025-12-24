# Kafka Integration Guide - Real-Time Sentiment Analysis

### 2.2 Configure Kafka

#### Edit `/usr/local/kafka/config/server.properties`

```bash
nano /usr/local/kafka/config/server.properties

# Key configurations:
broker.id=0
listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kafka-logs
num.partitions=3
log.retention.hours=168
zookeeper.connect=localhost:2181

# For better performance:
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
```

#### Edit `/usr/local/kafka/config/zookeeper.properties`

```bash
nano /usr/local/kafka/config/zookeeper.properties

# Key configurations:
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
```

### 2.3 Start Kafka Services

```bash
# Start Zookeeper (in terminal 1)
zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties

# Or run in background:
zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties

# Start Kafka Broker (in terminal 2)
kafka-server-start.sh /usr/local/kafka/config/server.properties

# Or run in background:
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties

# Verify services are running
jps
# You should see: QuorumPeerMain (Zookeeper) and Kafka
```

### 2.4 Create Kafka Topics

```bash
# Create topic for raw tweets
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tweets-raw

# create topic for training data
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tweets-training

# create topic for testing data
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tweets-testing

# create topic for results
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic sentiment-results

# List all topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe a topic
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic tweets-raw
```

### 3.3 Run the Producer

```bash
# Make executable
chmod +x tweet_producer.py

# Run producer (fast mode - 100 tweets/sec)
python3 tweet_producer.py \
  --csv-file ../data/raws/testdata.manual.2009.06.14.csv \
  --rate 10

# Run producer (real-time simulation - 10 tweets/sec)
python3 tweet_producer.py \
  --csv-file ../data/raws/training.1600000.processed.noemoticon.csv \
  --rate 10 \
  --realtime
```

### 3.4 Test Producer with Console Consumer

```bash
# In another terminal, consume messages
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic tweets-raw \
  --from-beginning \
  --max-messages 10

# You should see JSON messages like:
# {"ItemID":"12345","Sentiment":"1","SentimentSource":"Sentiment140","Text":"I love this product","timestamp":1699999999999}
```

---

## 4. Integrating with Hadoop MapReduce

Hadoop MapReduce is designed for batch processing, so we'll use Kafka to HDFS connector to bridge the gap.

### 4.1 Install Kafka Connect HDFS

```bash
# Download Confluent Kafka Connect HDFS
cd ~/Downloads
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-hdfs/versions/10.2.7/confluentinc-kafka-connect-hdfs-10.2.7.zip

# Extract
unzip confluentinc-kafka-connect-hdfs-10.2.7.zip
sudo mv confluentinc-kafka-connect-hdfs-10.2.7 /usr/local/kafka-connect-hdfs

# Copy to Kafka plugins directory
mkdir -p /usr/local/kafka/plugins
cp -r /usr/local/kafka-connect-hdfs /usr/local/kafka/plugins/
```

### 4.2 Configure Kafka Connect

Create `/usr/local/kafka/config/connect-standalone.properties`:

```properties
# Kafka Connect configuration
bootstrap.servers=localhost:9092

# Converter configs
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

# Offset storage
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000

# Plugin path
plugin.path=/usr/local/kafka/plugins
```

Create `/usr/local/kafka/config/hdfs-sink.properties`:

```properties
# HDFS Sink Connector
name=hdfs-sink-tweets
connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
tasks.max=3

# Topics to consume
topics=tweets-training,tweets-testing

# HDFS configuration
hdfs.url=hdfs://localhost:9000
flush.size=1000
rotate.interval.ms=60000

# Output format
format.class=io.confluent.connect.hdfs.json.JsonFormat
partitioner.class=io.confluent.connect.hdfs.partitioner.DefaultPartitioner

# Output paths
topics.dir=/user/kafka_data
logs.dir=/user/kafka_logs
```

### 4.3 Start Kafka Connect

```bash
# Start Kafka Connect in standalone mode
connect-standalone.sh \
  /usr/local/kafka/config/connect-standalone.properties \
  /usr/local/kafka/config/hdfs-sink.properties

# Data will be written to HDFS at:
# /user/kafka_data/tweets-training/
# /user/kafka_data/tweets-testing/
```

### 4.4 Modified Hadoop MapReduce to Read from HDFS Sink

Your existing Hadoop programs can now read from the Kafka-populated HDFS directories:

```bash
# Run Hadoop NB with Kafka data
hadoop jar nb.jar NB \
  /user/kafka_data/tweets-training \
  /user/kafka_data/tweets-testing \
  134217728 134217728

# Run Hadoop Modified NB with Kafka data
hadoop jar modified_nb.jar Modified_NB \
  /user/kafka_data/tweets-training \
  /user/kafka_data/tweets-testing \
  134217728 134217728
```

### 4.5 Alternative: Direct Kafka Consumer in Hadoop (Advanced)

For real-time processing, create a custom InputFormat:

Create `Hadoop/KafkaInputFormat.java`:

```java
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class KafkaInputFormat extends InputFormat<LongWritable, Text> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        // Create splits based on Kafka partitions
        List<InputSplit> splits = new ArrayList<>();
        String topic = context.getConfiguration().get("kafka.topic");
        int partitions = context.getConfiguration().getInt("kafka.partitions", 3);

        for (int i = 0; i < partitions; i++) {
            splits.add(new KafkaInputSplit(topic, i));
        }

        return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new KafkaRecordReader();
    }

    public static class KafkaRecordReader extends RecordReader<LongWritable, Text> {

        private KafkaConsumer<String, String> consumer;
        private Iterator<ConsumerRecord<String, String>> recordIterator;
        private LongWritable key;
        private Text value;
        private long recordsRead = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            KafkaInputSplit kafkaSplit = (KafkaInputSplit) split;

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "hadoop-consumer");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(props);
            // Subscribe to specific partition
            // Implementation details...
        }

        @Override
        public boolean nextKeyValue() {
            if (recordIterator == null || !recordIterator.hasNext()) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
                recordIterator = records.iterator();

                if (!recordIterator.hasNext()) {
                    return false;
                }
            }

            ConsumerRecord<String, String> record = recordIterator.next();
            key = new LongWritable(record.offset());
            value = new Text(record.value());
            recordsRead++;

            return true;
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return 0.5f; // Streaming has no end
        }

        @Override
        public void close() {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}

class KafkaInputSplit extends InputSplit implements org.apache.hadoop.mapreduce.InputSplit {
    private String topic;
    private int partition;

    public KafkaInputSplit(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

    // Getters and Writable implementation...
}
```

### 5.5 Compile and Run

```bash
cd ~/Personal/big_data_projects/Sentiment-Analysis-in-Hadoop-and-Spark/Spark

# Update build.sbt and add dependencies for json4s
echo 'libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.6"' >> build.sbt

# Compile
sbt clean compile package

# Train models offline (one-time)
spark-submit \
  --class TrainModels \
  --master "local[4]" \
  --driver-memory 4g \
  --executor-memory 4g \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

# Run Naive Bayes Streaming
spark-submit \
  --class NB_Streaming \
  --master "local[4]" \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

# Run SVM Streaming (in another terminal)
spark-submit \
  --class SVM_Streaming \
  --master "local[4]" \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar
```

---

### 6.2 Complete Workflow Steps

```bash
# 1. Start all services
chmod +x start_streaming_pipeline.sh
./start_streaming_pipeline.sh

# 2. Create Kafka topics (if not already created)
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic tweets-raw
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic tweets-training
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic tweets-testing
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic sentiment-results

# 3. Start Kafka Connect for Hadoop integration
connect-standalone.sh \
  /usr/local/kafka/config/connect-standalone.properties \
  /usr/local/kafka/config/hdfs-sink.properties &

# 4. Start Kafka Producer (in terminal 1)
cd kafka_producer
python3 tweet_producer.py \
  --csv-file ../data/raws/testdata.manual.2009.06.14.csv \
  --rate 100 \
  --train-ratio 0.8

# 5. Start Spark Streaming NB (in terminal 2)
spark-submit \
  --class NB_Streaming \
  --master local[4] \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

# 6. Start Spark Streaming SVM (in terminal 3)
spark-submit \
  --class SVM_Streaming \
  --master local[4] \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

# 7. Wait for Kafka Connect to write data to HDFS (few minutes)
# Check HDFS for accumulated data
hdfs dfs -ls /user/kafka_data/tweets-training

# 8. Run Hadoop MapReduce jobs on accumulated data
# Run Hadoop Basic NB (in terminal 4)
hadoop jar Hadoop/nb.jar NB \
  /user/kafka_data/tweets-training \
  /user/kafka_data/tweets-testing \
  134217728 134217728

# 9. Run Hadoop Modified NB (in terminal 5)
hadoop jar Hadoop/modified_nb.jar Modified_NB \
  /user/kafka_data/tweets-training \
  /user/kafka_data/tweets-testing \
  134217728 134217728
```
