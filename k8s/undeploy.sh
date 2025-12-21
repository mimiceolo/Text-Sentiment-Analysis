#!/bin/bash

# Script to undeploy/cleanup Text Sentiment Analysis from Kubernetes
# Usage: ./undeploy.sh [--keep-data]

set -e

NAMESPACE="sentiment-analysis"
KEEP_DATA=false
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-data)
            KEEP_DATA=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${RED}========================================${NC}"
echo -e "${RED}Undeploying Text Sentiment Analysis${NC}"
echo -e "${RED}========================================${NC}\n"

if [ "$KEEP_DATA" = true ]; then
    echo -e "${YELLOW}Mode: Keep PersistentVolumeClaims (data preserved)${NC}"
else
    echo -e "${RED}Mode: Delete everything including data${NC}"
fi

echo ""
read -p "Are you sure you want to continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 0
fi

echo -e "\n${YELLOW}Deleting jobs and cronjobs...${NC}"
kubectl delete -f spark-job.yaml --ignore-not-found=true
kubectl delete -f hadoop-job.yaml --ignore-not-found=true

echo -e "\n${YELLOW}Deleting applications...${NC}"
kubectl delete -f sentiment-api-deployment.yaml --ignore-not-found=true
kubectl delete -f kafka-producer-deployment.yaml --ignore-not-found=true

echo -e "\n${YELLOW}Deleting Spark cluster...${NC}"
kubectl delete -f spark-worker-deployment.yaml --ignore-not-found=true
kubectl delete -f spark-master-deployment.yaml --ignore-not-found=true

echo -e "\n${YELLOW}Deleting HDFS...${NC}"
kubectl delete -f hdfs-datanode-deployment.yaml --ignore-not-found=true
kubectl delete -f hdfs-namenode-deployment.yaml --ignore-not-found=true

echo -e "\n${YELLOW}Deleting Kafka and Zookeeper...${NC}"
kubectl delete -f kafka-deployment.yaml --ignore-not-found=true
kubectl delete -f zookeeper-deployment.yaml --ignore-not-found=true

echo -e "\n${YELLOW}Deleting MongoDB...${NC}"
kubectl delete -f mongodb-deployment.yaml --ignore-not-found=true

if [ "$KEEP_DATA" = false ]; then
    echo -e "\n${RED}Deleting PersistentVolumeClaims (data will be lost)...${NC}"
    kubectl delete pvc --all -n $NAMESPACE
fi

echo -e "\n${RED}Deleting namespace...${NC}"
kubectl delete namespace $NAMESPACE --ignore-not-found=true

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Cleanup completed!${NC}"
echo -e "${GREEN}========================================${NC}\n"

if [ "$KEEP_DATA" = true ]; then
    echo -e "${YELLOW}Note: PersistentVolumeClaims were preserved.${NC}"
    echo -e "${YELLOW}To delete them manually:${NC}"
    echo -e "  ${YELLOW}kubectl delete pvc --all -n $NAMESPACE${NC}\n"
fi
