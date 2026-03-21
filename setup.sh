#!/bin/bash

# 1. 공유 네트워크 생성 (이미 존재하면 에러 무시)
NETWORK_NAME="monitoring_net"

if [ -z "$(docker network ls --filter name=^${NETWORK_NAME}$ --format="{{.Name}}")" ]; then
  echo "Creating network: ${NETWORK_NAME}..."
  docker network create ${NETWORK_NAME}
else
  echo "Network ${NETWORK_NAME} already exists. Skipping..."
fi

# 2. EFK 스택 실행 (로그 수집 기반)
echo "Starting EFK Stack..."
cd ./EFK
docker-compose up -d --build
cd ..

# 3. Prometheus + Grafana 스택 실행 (메트릭 모니터링 기반)
echo "Starting Monitoring Stack (Prometheus/Grafana)..."
cd ./monitoring
docker-compose up -d --build
cd ..

# 4. Airflow 실행
echo "Starting Airflow Stack..."
# 루트에 있는 파일이 docker-compose.yml 이라면 -f 옵션 사용
docker-compose up -d 

echo "==========================================="
echo "All stacks are starting up!"
echo "Elasticsearch: http://localhost:9200"
echo "Kibana: http://localhost:5601"
echo "Prometheus: http://localhost:9090"
echo "Grafana: http://localhost:3000"
echo "Airflow: http://localhost:8080"
echo "==========================================="