.PHONY: proto build run test clean docker-up docker-down

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