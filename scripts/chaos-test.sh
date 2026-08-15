#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="deployments/docker-compose.yml"

wait_port() {
    local port="$1"
    for _ in $(seq 1 30); do
        if (echo > "/dev/tcp/localhost/${port}") 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "Timed out waiting for node on port ${port}" >&2
    return 1
}

echo "Starting distributed counter cluster for chaos tests..."
docker compose -f "${COMPOSE_FILE}" up -d --build

cleanup() {
    echo "Stopping distributed counter cluster..."
    docker compose -f "${COMPOSE_FILE}" down -v
}
trap cleanup EXIT

echo "Waiting for cluster startup..."
sleep 8

for port in 50051 50052 50053; do
    echo "Checking node on port ${port}..."
    wait_port "${port}"
done

echo "Running docker chaos test package..."
go test -count=1 -tags chaosdocker -timeout 120s -v ./test/chaos/...