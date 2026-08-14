.PHONY: proto build run test clean docker-up docker-down test-integration test-chaos test-chaos-docker bench

proto:
	@echo "Generating protobuf code..."
	./scripts/generate-proto.sh

build:
	@echo "Building application..."
	go build -o bin/counter-server ./cmd/server

run:
	@echo "Running application..."
	go run ./cmd/server -config configs/config.yaml

test:
	@echo "Running tests..."
	go test -v ./...

test-integration:
	@echo "Running integration tests..."
	"%SystemRoot%\\System32\\WindowsPowerShell\\v1.0\\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File ./scripts/test-integration.ps1

test-chaos:
	@echo "Running in-process chaos tests..."
	go test -count=1 -timeout 120s ./test/chaos/...

test-chaos-docker:
	@echo "Running docker chaos tests..."
	"%SystemRoot%\\System32\\WindowsPowerShell\\v1.0\\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File ./scripts/chaos-test.ps1

bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem -benchtime=2s ./test/benchmark/...

docker-up:
	@echo "Starting Docker Compose..."
	docker-compose -f deployments/docker-compose.yml up -d

docker-down:
	@echo "Stopping Docker Compose..."
	docker-compose -f deployments/docker-compose.yml down

docker-logs:
	docker-compose -f deployments/docker-compose.yml logs -f

clean:
	@echo "Cleaning..."
	rm -rf bin/
	go clean

init: proto
	@echo "✅ Setup complete! Run 'make run' to start the server"