#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "╔═══════════════════════════════════════════════╗"
echo "║         Go-Slipstream Benchmark Suite         ║"
echo "╚═══════════════════════════════════════════════╝"
echo ""

# --- Configuration ---
REQUESTS=${REQUESTS:-100000}
CONCURRENCY=${CONCURRENCY:-50}
DATA_SIZE=${DATA_SIZE:-256}
DATA_DIR="/tmp/slipstream-bench"

# --- Cleanup ---
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# --- Optional: Start Redis & MongoDB ---
REDIS_RUNNING=false
MONGO_RUNNING=false
if command -v podman &> /dev/null; then
    echo "Starting Redis container..."
    podman rm -f bench-redis 2>/dev/null || true
    podman run -d --name bench-redis -p 6379:6379 redis:alpine > /dev/null
    REDIS_RUNNING=true

    echo "Starting MongoDB container..."
    podman rm -f bench-mongo 2>/dev/null || true
    podman run -d --name bench-mongo -p 27017:27017 mongo:latest > /dev/null
    MONGO_RUNNING=true
    
    sleep 10 # Wait for Mongo to be ready
elif command -v docker &> /dev/null; then
    echo "Starting Redis container..."
    docker rm -f bench-redis 2>/dev/null || true
    docker run -d --name bench-redis -p 6379:6379 redis:alpine > /dev/null
    REDIS_RUNNING=true

    echo "Starting MongoDB container..."
    docker rm -f bench-mongo 2>/dev/null || true
    docker run -d --name bench-mongo -p 27017:27017 mongo:latest > /dev/null
    MONGO_RUNNING=true

    sleep 10
else
    echo "Warning: Neither podman nor docker found. Skipping remote benchmarks."
fi

# --- Build Benchmark Tool ---
echo "Building benchmark tool..."
cd "$PROJECT_ROOT"
go build -o "$PROJECT_ROOT/bench-tool" "$PROJECT_ROOT/cmd/bench/main.go"

# --- Run Benchmarks ---
echo ""
echo "Running benchmarks with:"
echo "  Requests:    $REQUESTS"
echo "  Concurrency: $CONCURRENCY"
echo "  Payload:     $DATA_SIZE bytes"
echo ""

TARGETS="slipstream,bolt,badger"
if [ "$REDIS_RUNNING" = true ]; then
    TARGETS="$TARGETS,redis"
fi
if [ "$MONGO_RUNNING" = true ]; then
    TARGETS="$TARGETS,mongodb"
fi

"$PROJECT_ROOT/bench-tool" \
    -n "$REQUESTS" \
    -c "$CONCURRENCY" \
    -d "$DATA_SIZE" \
    -data-dir "$DATA_DIR" \
    -mongo-addr "mongodb://localhost:27017" \
    -target "$TARGETS"

# --- Cleanup ---
echo ""
echo "Cleaning up..."
rm -rf "$DATA_DIR"
if [ "$REDIS_RUNNING" = true ] || [ "$MONGO_RUNNING" = true ]; then
    ENGINE="docker"
    command -v podman &> /dev/null && ENGINE="podman"
    
    [ "$REDIS_RUNNING" = true ] && $ENGINE rm -f bench-redis > /dev/null 2>&1 || true
    [ "$MONGO_RUNNING" = true ] && $ENGINE rm -f bench-mongo > /dev/null 2>&1 || true
fi

echo ""
echo "Benchmark complete."
