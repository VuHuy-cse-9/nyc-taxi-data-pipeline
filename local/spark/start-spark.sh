#!/usr/bin/env bash
set -e

echo "🚀 Starting Spark master..."

# If mode is master, start master; if worker, start worker
if [ "$MODE" == "master" ]; then
    echo "👷 Starting Spark master..."
    /opt/spark/sbin/start-master.sh
else
    echo "👷 Starting Spark worker..."
    /opt/spark/sbin/start-worker.sh spark://spark-master:7077
fi

echo "✅ Spark cluster started"

# Keep container alive
echo "🛌 Sleeping indefinitely..."
sleep infinity
