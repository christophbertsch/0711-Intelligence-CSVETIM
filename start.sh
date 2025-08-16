#!/bin/bash

# CSV Import Guardian Agent System Startup Script

set -e

echo "🚀 Starting CSV Import Guardian Agent System"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created. You may want to customize it for your environment."
fi

# Create sample data directory
mkdir -p sample_data

# Start infrastructure services first
echo "🏗️  Starting infrastructure services..."
docker-compose up -d zookeeper kafka postgres minio redis jaeger

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check if Kafka is ready
echo "🔍 Checking Kafka readiness..."
timeout=60
while ! docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    if [ $timeout -le 0 ]; then
        echo "❌ Kafka failed to start within timeout"
        exit 1
    fi
    echo "   Waiting for Kafka... ($timeout seconds remaining)"
    sleep 5
    timeout=$((timeout - 5))
done

# Check if PostgreSQL is ready
echo "🔍 Checking PostgreSQL readiness..."
timeout=60
while ! docker-compose exec -T postgres pg_isready -U guardian > /dev/null 2>&1; do
    if [ $timeout -le 0 ]; then
        echo "❌ PostgreSQL failed to start within timeout"
        exit 1
    fi
    echo "   Waiting for PostgreSQL... ($timeout seconds remaining)"
    sleep 5
    timeout=$((timeout - 5))
done

# Initialize database
echo "🗄️  Initializing database..."
docker-compose run --rm api python scripts/init_db.py

# Create sample CSV files
echo "📊 Creating sample CSV files..."
docker-compose run --rm api python scripts/create_sample_csv.py

# Start API and agents
echo "🤖 Starting API and agents..."
docker-compose up -d

# Wait a bit for everything to start
sleep 10

# Check service health
echo "🏥 Checking service health..."

# Check API health
if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ API is healthy"
else
    echo "⚠️  API health check failed"
fi

# Show service status
echo ""
echo "📋 Service Status:"
docker-compose ps

echo ""
echo "🎉 CSV Import Guardian Agent System is starting up!"
echo ""
echo "📍 Available endpoints:"
echo "   • API Documentation: http://localhost:8000/docs"
echo "   • API Health Check:  http://localhost:8000/health"
echo "   • MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
echo "   • Jaeger UI:         http://localhost:16686"
echo ""
echo "📁 Sample files created in: ./sample_data/"
echo ""
echo "🔧 To stop the system:"
echo "   docker-compose down"
echo ""
echo "📖 To view logs:"
echo "   docker-compose logs -f [service-name]"
echo ""
echo "🧪 To test the system:"
echo "   curl -X POST http://localhost:8000/v1/csv-import/upload \\"
echo "     -F \"file=@sample_data/fasteners_sample.csv\" \\"
echo "     -F \"client_id=demo\""