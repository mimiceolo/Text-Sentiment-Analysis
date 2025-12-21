#!/bin/bash

# Script to deploy Text Sentiment Analysis project to Kubernetes
# Usage: ./deploy.sh [--namespace sentiment-analysis]

set -e

NAMESPACE="sentiment-analysis"
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Text Sentiment Analysis - K8s Deployment${NC}"
echo -e "${GREEN}========================================${NC}\n"

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed${NC}"
    exit 1
fi

# Check if connected to cluster
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}Error: Not connected to a Kubernetes cluster${NC}"
    exit 1
fi

echo -e "${YELLOW}Current cluster:${NC}"
kubectl cluster-info | head -1
echo ""

read -p "Continue with deployment? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 0
fi

# Step 1: Create namespace
echo -e "\n${GREEN}[1/6] Creating namespace...${NC}"
kubectl apply -f namespace.yaml

# Step 2: Deploy storage infrastructure
echo -e "\n${GREEN}[2/6] Deploying storage components (MongoDB, Zookeeper, Kafka)...${NC}"
kubectl apply -f mongodb-deployment.yaml
kubectl apply -f zookeeper-deployment.yaml

# Wait for Zookeeper
echo -e "${YELLOW}Waiting for Zookeeper to be ready...${NC}"
kubectl wait --for=condition=ready pod -l app=zookeeper -n $NAMESPACE --timeout=120s

kubectl apply -f kafka-deployment.yaml

# Wait for Kafka
echo -e "${YELLOW}Waiting for Kafka to be ready...${NC}"
kubectl wait --for=condition=ready pod -l app=kafka -n $NAMESPACE --timeout=120s

# Step 3: Deploy HDFS
echo -e "\n${GREEN}[3/6] Deploying HDFS (NameNode, DataNode)...${NC}"
kubectl apply -f hdfs-namenode-deployment.yaml

echo -e "${YELLOW}Waiting for HDFS NameNode to be ready...${NC}"
kubectl wait --for=condition=ready pod -l app=hdfs-namenode -n $NAMESPACE --timeout=180s

kubectl apply -f hdfs-datanode-deployment.yaml

# Step 4: Deploy Spark
echo -e "\n${GREEN}[4/6] Deploying Spark cluster (Master, Workers)...${NC}"
kubectl apply -f spark-master-deployment.yaml

echo -e "${YELLOW}Waiting for Spark Master to be ready...${NC}"
kubectl wait --for=condition=ready pod -l app=spark-master -n $NAMESPACE --timeout=120s

kubectl apply -f spark-worker-deployment.yaml

# Step 5: Deploy applications
echo -e "\n${GREEN}[5/6] Deploying applications (API, Producer)...${NC}"
kubectl apply -f sentiment-api-deployment.yaml
kubectl apply -f kafka-producer-deployment.yaml

# Step 6: Deploy jobs (optional)
echo -e "\n${GREEN}[6/6] Deploying scheduled jobs...${NC}"
kubectl apply -f spark-job.yaml
kubectl apply -f hadoop-job.yaml

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment completed!${NC}"
echo -e "${GREEN}========================================${NC}\n"

# Show status
echo -e "${YELLOW}Current status:${NC}\n"
kubectl get pods -n $NAMESPACE

echo -e "\n${YELLOW}Services:${NC}\n"
kubectl get services -n $NAMESPACE

echo -e "\n${YELLOW}Storage:${NC}\n"
kubectl get pvc -n $NAMESPACE

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Next Steps:${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "1. Wait for all pods to be in Running state:"
echo -e "   ${YELLOW}kubectl get pods -n $NAMESPACE -w${NC}"
echo -e ""
echo -e "2. Get Sentiment API external IP:"
echo -e "   ${YELLOW}kubectl get service sentiment-api -n $NAMESPACE${NC}"
echo -e ""
echo -e "3. Access Spark Master UI:"
echo -e "   ${YELLOW}kubectl port-forward svc/spark-master 8080:8080 -n $NAMESPACE${NC}"
echo -e ""
echo -e "4. Access HDFS NameNode UI:"
echo -e "   ${YELLOW}kubectl port-forward svc/hdfs-namenode 9870:9870 -n $NAMESPACE${NC}"
echo -e ""
echo -e "5. Check API health:"
echo -e "   ${YELLOW}curl http://<API-EXTERNAL-IP>:5000/api/health${NC}"
echo -e "${GREEN}========================================${NC}\n"
