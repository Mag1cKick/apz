#!/bin/sh
set -e

CONSUL_ADDR="http://consul:8500"

echo "[INIT] Waiting for Consul to elect a leader..."
until curl -sf "${CONSUL_ADDR}/v1/status/leader" | grep -q '"'; do
  sleep 1
done
echo "[INIT] Consul is ready"

curl -sf -X PUT -d "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701" \
  "${CONSUL_ADDR}/v1/kv/hazelcast/cluster-members"
echo "[INIT] Set hazelcast/cluster-members"

curl -sf -X PUT -d "counter-queue" \
  "${CONSUL_ADDR}/v1/kv/mq/queue-name"
echo "[INIT] Set mq/queue-name"

echo "[INIT] Consul KV initialized successfully"
