# 🚀 Quick Start Guide - Text Sentiment Analysis on Kubernetes

Hướng dẫn này giúp bạn deploy toàn bộ hệ thống phân tích sentiment từ GitHub trong **15 phút**.

## 📋 Yêu cầu hệ thống

- **OS**: Windows 10/11, macOS, hoặc Linux
- **RAM**: Tối thiểu 16GB (khuyến nghị 32GB)
- **Disk**: 50GB trống
- **Docker Desktop** hoặc **Minikube**
- **kubectl** CLI

## ⚙️ Cài đặt công cụ

### Windows (PowerShell)

```powershell
# Cài đặt Chocolatey (nếu chưa có)
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Cài kubectl và minikube
choco install kubernetes-cli minikube -y

# Khởi động lại terminal
```

### macOS

```bash
# Cài Homebrew (nếu chưa có)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Cài kubectl và minikube
brew install kubectl minikube
```

### Linux (Ubuntu/Debian)

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

## 📦 Deployment từ GitHub

### Bước 1: Clone Repository

```bash
# Clone project
git clone https://github.com/mimiceolo/Text-Sentiment-Analysis.git
cd Text-Sentiment-Analysis

# Checkout branch phase2 (production-ready)
git checkout phase2
```

### Bước 2: Khởi động Kubernetes Cluster

#### Option A: Minikube (Khuyến nghị cho development)

```bash
# Start với 12GB RAM, 4 CPUs
minikube start --memory=12288 --cpus=4 --disk-size=50g

# Verify cluster
kubectl cluster-info
minikube status
```

#### Option B: Docker Desktop

```bash
# Settings → Kubernetes → Enable Kubernetes → Apply & Restart
# Chờ Kubernetes khởi động (Docker icon chuyển màu xanh)

# Verify
kubectl cluster-info
```

### Bước 3: Chuẩn bị Application Code

```bash
# Minikube: Mount sentiment API code
minikube ssh "sudo mkdir -p /hosthome/api"
minikube cp api/sentiment_api.py /hosthome/api/sentiment_api.py

# Docker Desktop: Sử dụng hostPath (tự động mount)
```

### Bước 4: Deploy Infrastructure

```bash
# Deploy toàn bộ stack (tất cả services cùng lúc)
kubectl apply -f k8s/

# Namespace được tạo sau đó services khác, nên chạy lại lần 2
kubectl apply -f k8s/
```

**Output mong đợi:**
```
namespace/sentiment-analysis created
persistentvolume/mongodb-pv created
persistentvolume/kafka-pv created
...
deployment.apps/spark-master created
deployment.apps/spark-worker created
```

### Bước 5: Kiểm tra Deployment

```bash
# Xem tất cả pods (đợi ~2-3 phút để pull images)
kubectl get pods -n sentiment-analysis

# Pods cần đạt trạng thái Running:
# - zookeeper
# - mongodb
# - hdfs-namenode
# - hdfs-datanode
# - kafka
# - kafka-producer
# - spark-master
# - spark-worker
# - sentiment-api (2 replicas)
```

**Kiểm tra chi tiết:**
```bash
# Xem logs nếu pod có lỗi
kubectl describe pod <pod-name> -n sentiment-analysis
kubectl logs <pod-name> -n sentiment-analysis

# Xem tất cả resources
kubectl get all -n sentiment-analysis
```

### Bước 6: Truy cập Services

#### Sentiment API (REST API)

**Minikube:**
```bash
# Tự động mở browser
minikube service sentiment-api -n sentiment-analysis
```

**Docker Desktop / Port Forward:**
```bash
kubectl port-forward -n sentiment-analysis svc/sentiment-api 5000:5000
```

Mở browser: `http://localhost:5000`

**API Endpoints:**
- `GET /` - API documentation
- `GET /api/health` - Health check
- `GET /api/predictions/recent?limit=100` - Recent predictions
- `GET /api/predictions/stats?hours=24` - Statistics
- `GET /api/metrics/batch` - Batch processing metrics

#### Spark Master UI

```bash
kubectl port-forward -n sentiment-analysis svc/spark-master 8080:8080
```

Mở: `http://localhost:8080`

#### HDFS NameNode UI

```bash
kubectl port-forward -n sentiment-analysis svc/hdfs-namenode 9870:9870
```

Mở: `http://localhost:9870`

## 🔄 Chạy Sentiment Analysis Pipeline

### 1. Upload dữ liệu vào HDFS

```bash
# Exec vào HDFS NameNode
kubectl exec -it -n sentiment-analysis deployment/hdfs-namenode -- bash

# Trong container:
hadoop fs -mkdir -p /sentiment/data
hadoop fs -put /data/training.csv /sentiment/data/
hadoop fs -ls /sentiment/data/
exit
```

### 2. Chạy Spark Streaming Job

```bash
# Chạy job manual
kubectl create job --from=cronjob/spark-sentiment-job spark-run-$(date +%s) -n sentiment-analysis

# Xem logs
kubectl logs -f job/spark-run-<timestamp> -n sentiment-analysis
```

### 3. Produce tweets vào Kafka

```bash
# Kafka producer đã tự động chạy
# Xem logs producer
kubectl logs -n sentiment-analysis deployment/kafka-producer
```

### 4. Kiểm tra kết quả trong MongoDB

```bash
# Connect MongoDB
kubectl exec -it -n sentiment-analysis deployment/mongodb -- mongosh

# Trong MongoDB shell:
use sentiment_analysis
db.predictions.find().limit(10)
db.batch_metrics.find().sort({timestamp: -1}).limit(5)
exit
```

## 🛠️ Quản lý Deployment

### Restart một service

```bash
kubectl rollout restart deployment/<deployment-name> -n sentiment-analysis

# Ví dụ:
kubectl rollout restart deployment/spark-master -n sentiment-analysis
```

### Scale services

```bash
# Tăng số lượng Spark workers
kubectl scale deployment/spark-worker --replicas=2 -n sentiment-analysis

# Tăng Sentiment API replicas
kubectl scale deployment/sentiment-api --replicas=3 -n sentiment-analysis
```

### Xem resource usage

```bash
# Enable metrics server (Minikube)
minikube addons enable metrics-server

# Xem usage
kubectl top nodes
kubectl top pods -n sentiment-analysis
```

### Update configuration

```bash
# Sau khi sửa YAML files
kubectl apply -f k8s/<file>.yaml

# Hoặc update toàn bộ
kubectl apply -f k8s/
```

## 🧹 Cleanup

### Xóa toàn bộ deployment

```bash
# Xóa namespace (xóa tất cả resources bên trong)
kubectl delete namespace sentiment-analysis

# Xóa persistent volumes
kubectl delete pv --all
```

### Dừng cluster

```bash
# Minikube
minikube stop

# Hoặc xóa hoàn toàn
minikube delete
```

## 🐛 Troubleshooting phổ biến

### 1. Pods bị `ImagePullBackOff`

**Nguyên nhân:** Docker image không tồn tại hoặc sai version

**Giải pháp:**
```bash
# Kiểm tra image name trong YAML
kubectl describe pod <pod-name> -n sentiment-analysis | grep Image

# Xóa pod để retry pull
kubectl delete pod <pod-name> -n sentiment-analysis
```

### 2. Pods bị `Pending` (Insufficient memory/cpu)

**Nguyên nhân:** Cluster không đủ resources

**Giải pháp:**
```bash
# Tăng RAM cho Minikube
minikube stop
minikube delete
minikube start --memory=16384 --cpus=6

# Hoặc giảm resource requests trong YAML
```

### 3. Sentiment API `CrashLoopBackOff`

**Nguyên nhân:** File `sentiment_api.py` chưa được mount

**Giải pháp:**
```bash
# Kiểm tra file
minikube ssh "ls -la /hosthome/api/"

# Copy lại
minikube cp api/sentiment_api.py /hosthome/api/sentiment_api.py

# Restart pods
kubectl delete pod -n sentiment-analysis -l app=sentiment-api
```

### 4. Kafka không start

**Giải pháp:**
```bash
# Xóa Kafka pod
kubectl delete pod -n sentiment-analysis -l app=kafka

# Đợi 30 giây để pod tự động recreate
watch kubectl get pods -n sentiment-analysis
```

### 5. Cannot connect to MongoDB

**Giải pháp:**
```bash
# Kiểm tra MongoDB logs
kubectl logs -n sentiment-analysis deployment/mongodb

# Restart MongoDB
kubectl rollout restart deployment/mongodb -n sentiment-analysis
```

## 📊 Monitoring & Logs

### Xem logs real-time

```bash
# Sentiment API
kubectl logs -f -n sentiment-analysis deployment/sentiment-api

# Spark Master
kubectl logs -f -n sentiment-analysis deployment/spark-master

# Kafka
kubectl logs -f -n sentiment-analysis deployment/kafka

# MongoDB
kubectl logs -f -n sentiment-analysis deployment/mongodb
```

### Dashboard (Minikube)

```bash
# Mở Kubernetes Dashboard
minikube dashboard
```

## 🎓 Tài liệu chi tiết

- [k8s/README.md](k8s/README.md) - Chi tiết từng component
- [k8s/OVERVIEW.md](k8s/OVERVIEW.md) - Kiến trúc hệ thống
- [k8s/SETUP-CLUSTER.md](k8s/SETUP-CLUSTER.md) - Setup production cluster

## 💡 Tips

1. **Development**: Dùng Minikube với `--memory=12288`
2. **Production**: Dùng managed Kubernetes (GKE, EKS, AKS) với autoscaling
3. **Monitoring**: Cài Prometheus + Grafana để theo dõi metrics
4. **Logs**: Dùng EFK stack (Elasticsearch, Fluentd, Kibana)
5. **CI/CD**: Tích hợp GitHub Actions để auto-deploy

## 🤝 Contribution

Mọi đóng góp đều được chào đón! Tạo Pull Request hoặc Issue trên GitHub.

## 📝 License

MIT License - Xem file LICENSE để biết thêm chi tiết.
