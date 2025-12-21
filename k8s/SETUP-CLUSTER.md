# Hướng dẫn Setup Kubernetes Cluster

## ⚠️ Yêu cầu trước khi deploy

Bạn cần có một Kubernetes cluster đang chạy. Chọn một trong các options sau:

## Option 1: Minikube (Đề xuất cho local development)

### Cài đặt Minikube:

**Windows (PowerShell as Admin):**
```powershell
# Sử dụng Chocolatey
choco install minikube

# Hoặc download trực tiếp
# https://minikube.sigs.k8s.io/docs/start/
```

### Khởi động Minikube:
```powershell
# Start với resource đủ cho project
minikube start --cpus=4 --memory=8192 --disk-size=50g

# Kiểm tra
kubectl cluster-info
kubectl get nodes
```

### Verify:
```powershell
kubectl config current-context
# Should show: minikube
```

---

## Option 2: Docker Desktop

### Setup:
1. Cài đặt Docker Desktop for Windows
2. Mở Docker Desktop
3. Settings → Kubernetes
4. ✓ Enable Kubernetes
5. Apply & Restart

### Verify:
```powershell
kubectl config current-context
# Should show: docker-desktop

kubectl cluster-info
```

---

## Option 3: Kind (Kubernetes in Docker)

### Cài đặt:
```powershell
# Sử dụng Chocolatey
choco install kind

# Hoặc go install
go install sigs.k8s.io/kind@latest
```

### Tạo cluster:
```powershell
# Tạo cluster với config tùy chỉnh
kind create cluster --name sentiment-analysis --config kind-config.yaml
```

**kind-config.yaml:**
```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
```

### Verify:
```powershell
kubectl config current-context
# Should show: kind-sentiment-analysis

kubectl get nodes
```

---

## Option 4: Cloud Kubernetes (Production)

### Google Kubernetes Engine (GKE):
```bash
gcloud container clusters create sentiment-cluster \
  --num-nodes=3 \
  --machine-type=n1-standard-4 \
  --zone=us-central1-a

gcloud container clusters get-credentials sentiment-cluster
```

### Amazon EKS:
```bash
eksctl create cluster \
  --name sentiment-cluster \
  --region us-east-1 \
  --nodegroup-name standard-workers \
  --node-type t3.xlarge \
  --nodes 3
```

### Azure AKS:
```bash
az aks create \
  --resource-group myResourceGroup \
  --name sentiment-cluster \
  --node-count 3 \
  --node-vm-size Standard_D4s_v3 \
  --enable-addons monitoring

az aks get-credentials --resource-group myResourceGroup --name sentiment-cluster
```

---

## Kiểm tra cluster đã sẵn sàng

```powershell
# Check cluster status
kubectl cluster-info

# Check nodes
kubectl get nodes

# Check current context
kubectl config current-context

# Check kubectl version
kubectl version --short
```

Output mong đợi:
```
Kubernetes control plane is running at https://...
CoreDNS is running at https://...

NAME       STATUS   ROLES           AGE   VERSION
minikube   Ready    control-plane   1m    v1.28.3
```

---

## Sau khi cluster đã sẵn sàng

Deploy project:
```powershell
# Deploy tất cả
kubectl apply -f k8s/

# Hoặc từng bước
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/mongodb-deployment.yaml
# ... (xem OVERVIEW.md)

# Kiểm tra
kubectl get pods -n sentiment-analysis
kubectl get svc -n sentiment-analysis
```

---

## Troubleshooting

### Lỗi: "No connection could be made"
```powershell
# Check Docker Desktop đang chạy
docker ps

# Restart Minikube
minikube stop
minikube start

# Hoặc reset context
kubectl config use-context minikube
```

### Lỗi: "current-context is not set"
```powershell
# List contexts
kubectl config get-contexts

# Set context
kubectl config use-context minikube
# hoặc
kubectl config use-context docker-desktop
```

### Cluster không đủ resources
```powershell
# Cho Minikube: tăng resources
minikube delete
minikube start --cpus=6 --memory=16384 --disk-size=100g

# Cho Kind: sửa config và recreate
kind delete cluster --name sentiment-analysis
kind create cluster --name sentiment-analysis --config kind-config-large.yaml
```

---

## Minimum System Requirements

Cho dự án này, cluster cần ít nhất:
- **CPU**: 4 cores (đề xuất 6-8 cores)
- **RAM**: 8GB (đề xuất 16GB)
- **Disk**: 50GB free space
- **Docker**: phiên bản mới nhất

---

## Next Steps

Sau khi cluster chạy thành công:

1. ✅ Verify cluster: `kubectl cluster-info`
2. ✅ Deploy namespace: `kubectl apply -f k8s/namespace.yaml`
3. ✅ Deploy infrastructure: MongoDB, Kafka, HDFS, Spark
4. ✅ Deploy applications: API, Producer
5. ✅ Check status: `kubectl get pods -n sentiment-analysis`

Xem chi tiết trong `k8s/README.md`
