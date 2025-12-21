# Text Sentiment Analysis - Kubernetes Deployment

Các file cấu hình Kubernetes để deploy dự án Text Sentiment Analysis lên K8s cluster.

## 📋 Tổng quan kiến trúc

Dự án bao gồm các thành phần sau:
- **MongoDB**: Lưu trữ kết quả phân tích sentiment
- **Kafka**: Message queue cho streaming data (với Zookeeper)
- **HDFS**: Hadoop Distributed File System (NameNode + DataNode)
- **Spark**: Xử lý real-time (Master + Workers)
- **Hadoop MapReduce**: Xử lý batch
- **Flask API**: REST API để truy vấn kết quả
- **Kafka Producer**: Stream tweets từ CSV

## 🚀 Cài đặt

### Bước 1: Tạo Namespace

```bash
kubectl apply -f k8s/namespace.yaml
```

### Bước 2: Deploy các thành phần cơ sở hạ tầng

**MongoDB:**
```bash
kubectl apply -f k8s/mongodb-deployment.yaml
```

**Zookeeper & Kafka:**
```bash
kubectl apply -f k8s/zookeeper-deployment.yaml
kubectl apply -f k8s/kafka-deployment.yaml
```

**HDFS:**
```bash
kubectl apply -f k8s/hdfs-namenode-deployment.yaml
kubectl apply -f k8s/hdfs-datanode-deployment.yaml
```

**Spark:**
```bash
kubectl apply -f k8s/spark-master-deployment.yaml
kubectl apply -f k8s/spark-worker-deployment.yaml
```

### Bước 3: Deploy các ứng dụng

**Sentiment API:**
```bash
kubectl apply -f k8s/sentiment-api-deployment.yaml
```

**Kafka Producer:**
```bash
kubectl apply -f k8s/kafka-producer-deployment.yaml
```

### Bước 4: Deploy các Jobs

**Spark Jobs:**
```bash
kubectl apply -f k8s/spark-job.yaml
```

**Hadoop MapReduce Jobs:**
```bash
kubectl apply -f k8s/hadoop-job.yaml
```

### Hoặc deploy tất cả cùng lúc:

```bash
kubectl apply -f k8s/
```

## 📊 Kiểm tra trạng thái

```bash
# Xem tất cả pods
kubectl get pods -n sentiment-analysis

# Xem tất cả services
kubectl get services -n sentiment-analysis

# Xem logs của một pod
kubectl logs -f <pod-name> -n sentiment-analysis

# Xem persistent volume claims
kubectl get pvc -n sentiment-analysis
```

## 🔧 Cấu hình

### Persistent Storage

Dự án sử dụng PersistentVolumeClaim cho:
- MongoDB: 20Gi
- Kafka: 10Gi
- Zookeeper: 5Gi
- HDFS NameNode: 20Gi
- HDFS DataNode: 50Gi

**Lưu ý**: Đảm bảo cluster của bạn có đủ storage và hỗ trợ dynamic provisioning hoặc tạo PersistentVolume trước.

### Resource Limits

Mỗi component đã được cấu hình với resource requests và limits phù hợp. Điều chỉnh trong file deployment nếu cần:

```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "1000m"
```

### Scaling

**Tăng số lượng Spark Workers:**
```bash
kubectl scale deployment spark-worker --replicas=5 -n sentiment-analysis
```

**Tăng số lượng API instances:**
```bash
kubectl scale deployment sentiment-api --replicas=3 -n sentiment-analysis
```

## 🌐 Truy cập Services

### Sentiment API

API được expose qua LoadBalancer. Lấy external IP:

```bash
kubectl get service sentiment-api -n sentiment-analysis
```

Sau đó truy cập:
- Health check: `http://<EXTERNAL-IP>:5000/api/health`
- Dashboard: `http://<EXTERNAL-IP>:5000/dashboard`
- API docs: `http://<EXTERNAL-IP>:5000/`

### Spark Master UI

Port-forward để truy cập Web UI:
```bash
kubectl port-forward svc/spark-master 8080:8080 -n sentiment-analysis
```
Truy cập: `http://localhost:8080`

### HDFS NameNode UI

```bash
kubectl port-forward svc/hdfs-namenode 9870:9870 -n sentiment-analysis
```
Truy cập: `http://localhost:9870`

### MongoDB

Kết nối từ bên trong cluster:
```
mongodb://mongodb.sentiment-analysis.svc.cluster.local:27017/sentiment_analysis
```

## 📦 Upload dữ liệu và code

### Upload CSV data vào Kafka Producer

```bash
# Copy CSV file vào pod
kubectl cp data/training.csv sentiment-analysis/kafka-producer-<pod-id>:/data/

# Exec vào pod và chạy producer
kubectl exec -it kafka-producer-<pod-id> -n sentiment-analysis -- bash
python3 /app/tweet_producer.py --csv-file /data/training.csv
```

### Upload Spark application

```bash
# Build Spark application (trên local)
cd Spark
sbt package

# Copy jar file vào Spark master
kubectl cp target/scala-2.12/sentiment-analysis_2.12-1.0.jar \
  sentiment-analysis/spark-master-<pod-id>:/opt/bitnami/spark/jars/
```

### Upload Hadoop MapReduce JAR

```bash
# Compile Java code (trên local)
cd Hadoop
javac -classpath $(hadoop classpath) NB.java
jar cf sentiment-nb.jar *.class

# Copy vào HDFS namenode
kubectl cp sentiment-nb.jar sentiment-analysis/hdfs-namenode-<pod-id>:/tmp/
```

## 🔄 Chạy Jobs

### Chạy Spark Job thủ công

```bash
kubectl create job --from=cronjob/spark-sentiment-job spark-manual-run -n sentiment-analysis
```

### Chạy Hadoop Job thủ công

```bash
kubectl create job --from=cronjob/hadoop-mapreduce-job hadoop-manual-run -n sentiment-analysis
```

### Xem logs của Job

```bash
# Spark
kubectl logs -f job/spark-manual-run -n sentiment-analysis

# Hadoop
kubectl logs -f job/hadoop-manual-run -n sentiment-analysis
```

## 🐛 Troubleshooting

### Pod không start

```bash
kubectl describe pod <pod-name> -n sentiment-analysis
kubectl logs <pod-name> -n sentiment-analysis
```

### Storage issues

```bash
# Xem PVC status
kubectl get pvc -n sentiment-analysis

# Describe PVC để xem lỗi
kubectl describe pvc <pvc-name> -n sentiment-analysis
```

### Network issues

```bash
# Test kết nối MongoDB từ API pod
kubectl exec -it sentiment-api-<pod-id> -n sentiment-analysis -- \
  ping mongodb

# Test Kafka connection
kubectl exec -it kafka-producer-<pod-id> -n sentiment-analysis -- \
  nc -zv kafka 9092
```

## 🗑️ Xóa toàn bộ deployment

```bash
kubectl delete namespace sentiment-analysis
```

Hoặc xóa từng thành phần:
```bash
kubectl delete -f k8s/
```

## 📝 Lưu ý quan trọng

1. **Thứ tự triển khai**: Deploy infrastructure components trước (MongoDB, Kafka, HDFS, Spark), sau đó mới deploy applications
2. **Init time**: Một số services cần thời gian khởi động (đặc biệt là HDFS và Spark). Đợi pods ở trạng thái Running trước khi tiếp tục
3. **Dependencies**: Đảm bảo Zookeeper đã sẵn sàng trước khi start Kafka
4. **ConfigMaps**: Cần tạo ConfigMaps chứa code Python/Scala nếu muốn tự động mount code vào pods
5. **Images**: Có thể cần build custom Docker images chứa sẵn application code và dependencies

## 🔐 Security Notes

- Cấu hình hiện tại không có authentication/authorization (phù hợp cho development/testing)
- Cho production, cần thêm:
  - MongoDB authentication
  - Kafka SASL/SSL
  - HDFS Kerberos
  - API authentication (JWT, OAuth)
  - Network Policies
  - Secrets cho sensitive data

## 🚀 Production Best Practices

1. Sử dụng StatefulSets cho stateful apps (Kafka, HDFS)
2. Configure proper monitoring (Prometheus, Grafana)
3. Setup logging aggregation (ELK stack)
4. Use Ingress thay vì LoadBalancer cho API
5. Implement auto-scaling (HPA)
6. Regular backups cho MongoDB và HDFS
7. Use Helm charts để quản lý deployments

## 📚 Tài liệu tham khảo

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Hadoop on Kubernetes](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/DockerContainers.html)
