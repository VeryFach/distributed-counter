#!/bin/bash

echo "Testing Distributed Counter Cluster"

# Test increment on Node A
echo "Testing increment..."
grpcurl -plaintext -d '{"delta": 5}' localhost:50051 counter.CounterService/Increment

# Test get value from Node B
echo "Getting value from Node B..."
grpcurl -plaintext localhost:50052 counter.CounterService/GetValue

# Test decrement on Node C
echo "Testing decrement on Node C..."
grpcurl -plaintext -d '{"delta": 3}' localhost:50053 counter.CounterService/Decrement

# Check consistency across nodes
echo "Checking consistency across nodes..."
for port in 50051 50052 50053; do
    echo "Node $port:"
    grpcurl -plaintext localhost:$port counter.CounterService/GetValue
done

# Load testing with 100 concurrent increments
echo "Running load test..."
for i in {1..100}; do
    grpcurl -plaintext -d '{"delta": 1}' localhost:50051 counter.CounterService/Increment &
done
wait

echo "Final counter value:"
grpcurl -plaintext localhost:50051 counter.CounterService/GetValue