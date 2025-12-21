# Text Sentiment Analysis - Kubernetes Deployment Files

## 📁 Cấu trúc thư mục

```
k8s/
├── README.md                          # Hướng dẫn chi tiết
├── deploy.sh                          # Script tự động deploy
├── undeploy.sh                        # Script xóa deployment
├── namespace.yaml                     # Namespace definition
├── mongodb-deployment.yaml            # MongoDB database
├── zookeeper-deployment.yaml          # Zookeeper for Kafka
├── kafka-deployment.yaml              # Kafka message broker
├── hdfs-namenode-deployment.yaml      # HDFS NameNode
├── hdfs-datanode-deployment.yaml      # HDFS DataNode
├── spark-master-deployment.yaml       # Spark Master
├── spark-worker-deployment.yaml       # Spark Workers
├── sentiment-api-deployment.yaml      # Flask REST API
├── kafka-producer-deployment.yaml     # Kafka Producer
├── spark-job.yaml                     # Spark scheduled jobs
└── hadoop-job.yaml                    # Hadoop MapReduce jobs
```

## 🚀 Quick Start

### Cách 1: Sử dụng script tự động (Linux/Mac)

```bash
cd k8s
chmod +x deploy.sh
./deploy.sh
```

### Cách 2: Deploy thủ công (Windows/Linux/Mac)

```bash
# Apply tất cả files
kubectl apply -f k8s/

# Hoặc từng bước
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/mongodb-deployment.yaml
kubectl apply -f k8s/zookeeper-deployment.yaml
kubectl apply -f k8s/kafka-deployment.yaml
kubectl apply -f k8s/hdfs-namenode-deployment.yaml
kubectl apply -f k8s/hdfs-datanode-deployment.yaml
kubectl apply -f k8s/spark-master-deployment.yaml
kubectl apply -f k8s/spark-worker-deployment.yaml
kubectl apply -f k8s/sentiment-api-deployment.yaml
kubectl apply -f k8s/kafka-producer-deployment.yaml
kubectl apply -f k8s/spark-job.yaml
kubectl apply -f k8s/hadoop-job.yaml
```

## 📊 Kiểm tra deployment

```bash
# Xem trạng thái pods
kubectl get pods -n sentiment-analysis

# Xem services
kubectl get svc -n sentiment-analysis

# Xem logs
kubectl logs -f <pod-name> -n sentiment-analysis
```

## 🗑️ Xóa deployment

```bash
# Sử dụng script (giữ lại data)
./undeploy.sh --keep-data

# Hoặc xóa hoàn toàn
./undeploy.sh

# Hoặc thủ công
kubectl delete namespace sentiment-analysis
```

## 📝 Các thành phần

| Component | Replicas | Resources | Storage |
|-----------|----------|-----------|---------|
| MongoDB | 1 | 512Mi-2Gi / 0.5-1 CPU | 20Gi |
| Zookeeper | 1 | 256Mi-512Mi / 0.25-0.5 CPU | 5Gi |
| Kafka | 1 | 1Gi-2Gi / 0.5-1 CPU | 10Gi |
| HDFS NameNode | 1 | 1Gi-2Gi / 0.5-1 CPU | 20Gi |
| HDFS DataNode | 2 | 1Gi-2Gi / 0.5-1 CPU | 50Gi |
| Spark Master | 1 | 1Gi-2Gi / 0.5-1 CPU | - |
| Spark Worker | 3 | 2Gi-4Gi / 1-2 CPU | - |
| Sentiment API | 2 | 256Mi-512Mi / 0.25-0.5 CPU | - |
| Kafka Producer | 1 | 256Mi-512Mi / 0.25-0.5 CPU | - |

## 🔗 Truy cập Services

Xem README.md trong thư mục k8s để biết chi tiết về cách truy cập các services.

## ⚠️ Lưu ý

1. Cluster cần có đủ resources (ít nhất 16GB RAM, 8 CPU cores)
2. Cần có StorageClass hỗ trợ dynamic provisioning hoặc tạo PV trước
3. Một số pods cần thời gian khởi động (đặc biệt HDFS và Spark)
4. Đọc k8s/README.md để biết hướng dẫn chi tiết
