#!/bin/bash
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Health Check Script for IDOP Ingestion Layer
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

check_service() {
    local name="$1"
    local url="$2"
    local expected="${3:-200}"

    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null || echo "000")
    if [ "$status" = "$expected" ]; then
        echo -e "  ${GREEN}âœ“${NC} ${name} (HTTP ${status})"
        return 0
    else
        echo -e "  ${RED}âœ—${NC} ${name} (HTTP ${status}, expected ${expected})"
        return 1
    fi
}

echo "â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•"
echo " IDOP Ingestion Layer Health Check"
echo "â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•"
echo ""

failures=0

echo "â”€â”€ Envoy Load Balancer â”€â”€"
check_service "Envoy Admin" "http://localhost:9901/ready" || ((failures++))

echo ""
echo "â”€â”€ Vector Aggregator â”€â”€"
check_service "Vector Health" "http://localhost:8686/health" || ((failures++))   # FIX: was 8687 — /health is on Vector API port 8686

echo ""
echo "â”€â”€ Redpanda â”€â”€"
check_service "Redpanda Admin" "http://localhost:9644/v1/cluster/health" || ((failures++))
check_service "Schema Registry" "http://localhost:18081/subjects" || ((failures++))

echo ""
echo "â”€â”€ ClickHouse â”€â”€"
check_service "ClickHouse HTTP" "http://localhost:8123/ping" || ((failures++))

echo ""
echo "â”€â”€ Apache Flink â”€â”€"
check_service "Flink Web UI" "http://localhost:8081/overview" || ((failures++))

echo ""
echo "â”€â”€ Monitoring â”€â”€"
check_service "Prometheus" "http://localhost:9090/-/healthy" || ((failures++))
check_service "Grafana" "http://localhost:3000/api/health" || ((failures++))

echo ""
if [ $failures -gt 0 ]; then
    echo -e "${RED}â•â•â• ${failures} service(s) unhealthy â•â•â•${NC}"
    exit 1
else
    echo -e "${GREEN}â•â•â• All services healthy â•â•â•${NC}"
    exit 0
fi
