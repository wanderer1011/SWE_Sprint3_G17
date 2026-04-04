#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Health Check Script for IDOP Ingestion Layer
# ─────────────────────────────────────────────────────────────
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
        echo -e "  ${GREEN}✓${NC} ${name} (HTTP ${status})"
        return 0
    else
        echo -e "  ${RED}✗${NC} ${name} (HTTP ${status}, expected ${expected})"
        return 1
    fi
}

echo "═══════════════════════════════════════════"
echo " IDOP Ingestion Layer Health Check"
echo "═══════════════════════════════════════════"
echo ""

failures=0

echo "── Envoy Load Balancer ──"
check_service "Envoy Admin" "http://localhost:9901/ready" || ((failures++))

echo ""
echo "── Vector Aggregator ──"
check_service "Vector Health" "http://localhost:8686/health" || ((failures++))

echo ""
echo "── Redpanda ──"
check_service "Redpanda Admin" "http://localhost:9644/v1/cluster/health_overview" || ((failures++))
check_service "Schema Registry" "http://localhost:18081/subjects" || ((failures++))

echo ""
echo "── ClickHouse ──"
check_service "ClickHouse HTTP" "http://localhost:8123/ping" || ((failures++))

echo ""
echo "── Apache Flink ──"
check_service "Flink Web UI" "http://localhost:8081/overview" || ((failures++))

echo ""
echo "── Monitoring ──"
check_service "Prometheus" "http://localhost:9090/-/healthy" || ((failures++))
check_service "Grafana" "http://localhost:3000/api/health" || ((failures++))

echo ""
if [ $failures -gt 0 ]; then
    echo -e "${RED}═══ ${failures} service(s) unhealthy ═══${NC}"
    exit 1
else
    echo -e "${GREEN}═══ All services healthy ═══${NC}"
    exit 0
fi
