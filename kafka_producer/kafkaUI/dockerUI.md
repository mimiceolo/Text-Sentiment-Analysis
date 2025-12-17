1. Check docker0 IP address
ip addr show docker0

2. Run docker UI
docker run \
  --add-host=172.17.0.1:host-gateway \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=172.17.0.1:9092 \
  provectuslabs/kafka-ui:latest

