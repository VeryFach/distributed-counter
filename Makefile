.PHONY: proto build run test test-integration test-e2e test-chaos test-chaos-docker bench clean docker-up docker-down docker-logs init

proto:
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/proto/counter.proto

build:
	@echo "Building application..."
	go build -o bin/counter-server ./cmd/server

run:
	@echo "Running application..."
	go run ./cmd/server -config configs/config.yaml

test:
	@echo "Running in-process tests (no Docker required)..."
	go test -count=1 -timeout 180s ./internal/... ./test/partition/... ./test/chaos/... ./test/multicounter/... ./test/election/...

test-integration:
	@echo "Running integration tests..."
	"%SystemRoot%\\System32\\WindowsPowerShell\\v1.0\\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File ./scripts/test-integration.ps1

test-e2e:
	@echo "Running e2e tests..."
	go test -count=1 -timeout 120s ./test/e2e/...

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