# Sentiment Analysis in Hadoop and Spark

A comprehensive **Big Data processing pipeline** for real-time sentiment analysis of Twitter data using **Apache Spark (NaiveBayes and SVM)**, **Kafka**, and **MongoDB**. This project demonstrates distributed computing approaches for classifying tweet sentiment (positive/negative) at scale.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Performance](#performance)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project implements a complete end-to-end sentiment analysis pipeline that:

- **Ingests** tweet data via Kafka producers
- **Processes** data using Apache Spark
- **Trains** machine learning models (Naive Bayes and SVM)
- **Streams** real-time predictions to MongoDB
- **Exposes** results via REST API
- **Deploys** on Kubernetes for scalability

## 📊 Datasets

This project uses two datasets for training and testing:

### 1. Sentiment140 (Training & Testing)
- **Source**: [Twitter Sentiment Analysis Training Corpus](http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/)
- **Size**: 1.6 million tweets
- **Format**: `Sentiment, ID, Date, Query, User, Text` (6 columns)
- **Sentiment Labels**: 0 (negative), 4 (positive), 2 (neutral - filtered out)
- **Usage**: Primary training dataset and testing data

### 2. TweetEval – Sentiment (Testing)
- **Source**: [TweetEval Benchmark](https://github.com/cardiffnlp/tweeteval) - Hugging Face Datasets
- **Format**: `ItemID, Sentiment, SentimentSource, SentimentText` (4 columns)
- **Sentiment Labels**: 0 (negative), 1 (positive), 1 (neutral - filtered out)
- **Usage**: Additional testing dataset for model evaluation
- **Download**: Use `data/raws/download_test_data.py` to download and convert to CSV format

### Supported Data Formats

- **Sentiment140 format**: `Sentiment, ID, Date, Query, User, Text` (6 columns)
- **TweetEval/Simple format**: `ItemID, Sentiment, SentimentSource, SentimentText` (4 columns)

## 🏗️ Architecture

```
┌─────────────┐
│ CSV Data    │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌─────────────┐
│ Kafka        │────▶│ Spark       │
│ Producer     │     │ Streaming   │
└─────────────┘     └──────┬──────┘
                            │
                            ▼
                    ┌─────────────┐
                    │ MongoDB     │
                    │ (Predictions│
                    │  & Metrics) │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ Flask API    │
                    │ (REST)       │
                    └─────────────┘
```

### Components

1. **Data Ingestion**: Kafka Producer reads CSV files and publishes to `tweets-testing` topic
2. **Batch Processing**: Hadoop MapReduce for offline training and prediction
3. **Stream Processing**: Apache Spark Streaming for real-time sentiment analysis
4. **Model Training**: Spark MLlib (Naive Bayes & SVM) trained on HDFS data
5. **Storage**: MongoDB for predictions and batch metrics
6. **API Layer**: Flask REST API for querying results
7. **Orchestration**: Kubernetes for containerized deployment

## ✨ Features

### Core Capabilities

- ✅ **Multi-framework Support**: Both Hadoop MapReduce and Apache Spark implementations
- ✅ **Real-time Streaming**: Kafka + Spark Streaming for live predictions
- ✅ **Multiple ML Models**: Naive Bayes and Support Vector Machine (SVM)
- ✅ **Distributed Processing**: Scales to handle millions of tweets
- ✅ **RESTful API**: Query predictions, metrics, and statistics
- ✅ **Kubernetes Ready**: Complete K8s deployment configurations
- ✅ **Flexible Data Formats**: Supports multiple CSV input formats

### Text Preprocessing

All implementations use consistent regex-based cleaning:

- Remove URLs (`https?://...`)
- Remove mentions/hashtags (`@user`, `#hashtag`)
- Remove numbers and punctuation
- Convert to lowercase and trim whitespace

## 🛠️ Technologies

| Category                | Technology                          |
| ----------------------- | ----------------------------------- |
| **Big Data Frameworks** | Apache Hadoop, Apache Spark         |
| **Streaming**           | Apache Kafka, Spark Streaming       |
| **Storage**             | HDFS, MongoDB                       |
| **Languages**           | Java, Scala, Python                 |
| **ML Libraries**        | Spark MLlib (NaiveBayes, LinearSVC) |
| **API Framework**       | Flask (Python)                      |
| **Orchestration**       | Kubernetes, Docker                  |
| **Build Tools**         | sbt (Scala), Maven (Java)           |

## 📁 Project Structure

```
Sentiment-Analysis-in-Hadoop-and-Spark/
├── api/                          # Flask REST API
│   ├── sentiment_api.py          # Main API server
│   ├── aggregate_metrics.py     # Metrics aggregation
│   └── templates/
│       └── dashboard.html        # Web dashboard
│
├── Spark/                        # Apache Spark implementations
│   ├── src/main/scala/
│   │   ├── TrainModels.scala    # Model training (NB & SVM)
│   │   ├── NB_Streaming_MongoDB.scala    # NB streaming
│   │   └── SVM_Streaming_MongoDB.scala   # SVM streaming
│   ├── scripts/                  # Helper scripts
│   └── build.sbt                # Scala build config
│
├── kafka_producer/              # Kafka data producer
│   ├── tweet_producer.py        # CSV to Kafka producer
│   └── requirements.txt
│
├── k8s/                          # Kubernetes deployments
│   ├── namespace.yaml
│   ├── mongodb-deployment.yaml
│   ├── kafka-deployment.yaml
│   ├── spark-master-deployment.yaml
│   ├── spark-worker-deployment.yaml
│   ├── sentiment-api-deployment.yaml
│   └── ...
│
├── data/                        # Data directory
│   ├── raws/                    # Raw CSV files
│   │   └── download_test_data.py  # Script to download TweetEval dataset
│   └── preprocessed/            # Preprocessed data
│
├── docs/                        # Documentation
│   ├── STREAMING_GUIDE.md       # Streaming setup guide
│   ├── kafka/docs.md            # Kafka documentation
│   └── mongodb/docs.md          # MongoDB documentation
│
├── preprocess_sentiment_data.py # Data preprocessing script
├── REQUIREMENTS.txt             # Python dependencies
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites

- **Java 8+** (for Hadoop/Spark)
- **Scala 2.12+** and **sbt** (for Spark apps)
- **Python 3.8+** (for Kafka producer and API)
- **Hadoop 3.x** (HDFS running)
- **Spark 3.x**
- **Kafka 2.x**
- **MongoDB 4.x+**
- **Docker** (optional, for containerized deployment)
- **Kubernetes** (optional, for K8s deployment)

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/Sentiment-Analysis-in-Hadoop-and-Spark.git
cd Sentiment-Analysis-in-Hadoop-and-Spark
```

### 2. Install Python Dependencies

```bash
pip install -r REQUIREMENTS.txt
pip install -r kafka_producer/requirements.txt
pip install -r api/requirements.txt
```

### 3. Start Services

```bash
# Start HDFS
start-dfs.sh
start-yarn.sh

# Start Kafka
zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties

# Verify services
jps

# Start MongoDB
mongod --dbpath /path/to/data/db
```

### 4. Prepare Data

```bash
# Option 1: Download TweetEval dataset
cd data/raws
python3 download_test_data.py
# This will create tweeteval_sentiment_test.csv

# Option 2: Place your CSV file in data/raws/
# Supported formats:
# - Sentiment140: Sentiment,ID,Date,Query,User,Text (6 columns)
# - TweetEval/Simple: ItemID,Sentiment,SentimentSource,SentimentText (4 columns)

# Prepare training data (creates HDFS directories and uploads data)
cd Spark/scripts
./prepare_training_data.sh
```

### 5. Train Models (Spark)

```bash
cd Spark
sbt package

# Train both NB and SVM models
spark-submit \
  --class TrainModels \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar
```

### 6. Start Kafka Producer

```bash
# Produce tweets to Kafka
# You can use either Sentiment140 or TweetEval test data
python3 kafka_producer/tweet_producer.py \
  --csv-file data/raws/testdata.manual.2009.06.14.csv \
  --rate 50 \
  --bootstrap-servers localhost:9092

# Or use TweetEval dataset
python3 kafka_producer/tweet_producer.py \
  --csv-file data/raws/tweeteval_sentiment_test.csv \
  --rate 50 \
  --bootstrap-servers localhost:9092
```

### 7. Start Spark Streaming

```bash
# Start Naive Bayes streaming
spark-submit \
  --class NB_Streaming_MongoDB \
  --master local[*] \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.mongodb:mongodb-driver-sync:4.11.1 \
  Spark/target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

# Or start SVM streaming (in another terminal)
spark-submit \
  --class SVM_Streaming_MongoDB \
  --master local[*] \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.12,org.mongodb:mongodb-driver-sync:4.11.1 \
  Spark/target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar
```

### 8. Start API Server

```bash
# Start Flask API
cd api
python3 sentiment_api.py

# API will be available at http://localhost:5000
```

## 📖 Detailed Setup

For detailed setup instructions, see:

- **[QUICK_START.md](QUICK_START.md)** - Kubernetes deployment guide
- **[docs/STREAMING_GUIDE.md](docs/STREAMING_GUIDE.md)** - Streaming setup guide
- **[k8s/README.md](k8s/README.md)** - Kubernetes deployment details
- **[k8s/OVERVIEW.md](k8s/OVERVIEW.md)** - Architecture overview

## 💻 Usage

### Kafka Producer

```bash
python3 kafka_producer/tweet_producer.py \
  --csv-file <path-to-csv> \
  --rate <messages-per-second> \
  --bootstrap-servers <kafka-broker> \
  [--verbose]
```

**Options:**

- `--csv-file`: Path to CSV file (default: `../data/raws/testdata.manual.2009.06.14.csv`)
  - Supports Sentiment140 format: `Sentiment,ID,Date,Query,User,Text`
  - Supports TweetEval format: `ItemID,Sentiment,SentimentSource,SentimentText`
- `--rate`: Messages per second (default: 100)
- `--bootstrap-servers`: Kafka broker address (default: `localhost:9092`)
- `--verbose`: Enable debug logging

**Example:**
```bash
# Using Sentiment140 test data
python3 kafka_producer/tweet_producer.py --csv-file data/raws/testdata.manual.2009.06.14.csv

# Using TweetEval test data
python3 kafka_producer/tweet_producer.py --csv-file data/raws/tweeteval_sentiment_test.csv
```

### Spark Streaming

The streaming applications consume from Kafka topic `tweets-testing` and write predictions to MongoDB:

- **Collection**: `predictions` - Individual tweet predictions
- **Collection**: `batch_metrics` - Batch processing metrics

### Data Preprocessing

```bash
# Preprocess Sentiment140 dataset (converts 0/4 labels to 0/1)
python3 preprocess_sentiment_data.py \
  input.csv \
  output.csv

# Download TweetEval dataset (already in correct format)
cd data/raws
python3 download_test_data.py
# Creates tweeteval_sentiment_test.csv with ItemID,Sentiment,SentimentSource,SentimentText format
```

## 📡 API Documentation

### Base URL

```
http://localhost:5000
```

### Endpoints

#### Health Check

```http
GET /api/health
```

**Response:**

```json
{
  "status": "healthy",
  "mongodb": "connected",
  "collections": {
    "predictions": 12345,
    "batch_metrics": 100
  }
}
```

#### Recent Predictions

```http
GET /api/predictions/recent?limit=100&model=svm
```

**Query Parameters:**

- `limit`: Number of results (default: 100)
- `model`: Filter by model (`svm`, `naive_bayes`, or omit for all)

#### Prediction Statistics

```http
GET /api/predictions/stats?hours=24&model=svm
```

**Query Parameters:**

- `hours`: Time window (default: 24)
- `model`: Filter by model

#### Batch Metrics

```http
GET /api/metrics/batch?limit=10&model=svm
```

#### Sentiment Trend

```http
GET /api/sentiment/trend?hours=24&interval=1h
```

#### Model Comparison

```http
GET /api/models/compare?hours=24
```

#### Search Tweets

```http
GET /api/search/tweets?q=keyword&limit=50
```

### Example Usage

```bash
# Get recent predictions
curl http://localhost:5000/api/predictions/recent?limit=10

# Get statistics for last 24 hours
curl http://localhost:5000/api/predictions/stats?hours=24

# Compare models
curl http://localhost:5000/api/models/compare?hours=24
```

## 📊 Performance

### Model Performance

| Model           | Training Time | Accuracy | Precision | Recall | F1-Score |
| --------------- | ------------- | -------- | --------- | ------ | -------- |
| **Naive Bayes** | ~5-8 min      | ~76%     | ~0.77     | ~0.76  | ~0.76    |
| **SVM**         | ~8-12 min     | ~78%     | ~0.79     | ~0.78  | ~0.78    |

_Note: Performance metrics are approximate and depend on cluster configuration and dataset size._

### Throughput

- **Kafka Producer**: Configurable rate (default: 100 msgs/sec)
- **Spark Streaming**: Processes batches every 10 seconds
- **MongoDB Write**: Bulk writes for optimal performance

### Scalability

- **Horizontal Scaling**: Add Spark workers for increased throughput
- **Kafka Partitions**: Increase partitions for parallel processing
- **MongoDB Sharding**: Shard collections for large datasets

## 🚢 Deployment

### Kubernetes Deployment

See **[QUICK_START.md](QUICK_START.md)** for complete Kubernetes deployment guide.

**Quick Deploy:**

```bash
# Deploy all components
kubectl apply -f k8s/

# Check status
kubectl get pods -n sentiment-analysis

# Access API
kubectl port-forward -n sentiment-analysis svc/sentiment-api 5000:5000
```

### Docker Compose (Alternative)

```bash
# Build and start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## 🔧 Configuration

### Kafka Topics

- `tweets-testing`: Testing data for model predictions

### MongoDB Collections

- `predictions`: Individual tweet predictions with metadata
- `batch_metrics`: Batch processing metrics (accuracy, F1-score, etc.)

### HDFS Paths

- `/user/spark/models/`: Trained model storage
- `/user/spark/checkpoints_*/`: Spark Streaming checkpoints
- `/user/spark/streaming_results/`: Streaming output backups

## 🧪 Testing

```bash
# Test Kafka producer
python3 kafka_producer/tweet_producer.py --csv-file test.csv --rate 10

# Test API
curl http://localhost:5000/api/health

# Test MongoDB connection
mongosh
use sentiment_analysis
db.predictions.find().limit(5)
```

## 📚 Documentation

- **[docs/STREAMING_GUIDE.md](docs/STREAMING_GUIDE.md)** - Streaming setup and usage
- **[docs/kafka/docs.md](docs/kafka/docs.md)** - Kafka configuration
- **[docs/mongodb/docs.md](docs/mongodb/docs.md)** - MongoDB schema and queries
- **[k8s/README.md](k8s/README.md)** - Kubernetes deployment guide
- **[k8s/OVERVIEW.md](k8s/OVERVIEW.md)** - System architecture overview

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow code style conventions for each language
- Add tests for new features
- Update documentation as needed
- Ensure backward compatibility

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Datasets**:
  - [Twitter Sentiment Analysis Training Corpus (Sentiment140)](http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/)
  - [TweetEval Benchmark](https://github.com/cardiffnlp/tweeteval) - Hugging Face Datasets
- **Frameworks**: Apache Hadoop, Apache Spark, Apache Kafka
- **Libraries**: Spark MLlib, MongoDB Driver, Flask

## 📞 Support

For issues, questions, or contributions:

- **Issues**: [GitHub Issues](https://github.com/yourusername/Sentiment-Analysis-in-Hadoop-and-Spark/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/Sentiment-Analysis-in-Hadoop-and-Spark/discussions)

---

**Built with ❤️ using Big Data technologies**
