#!/bin/sh
# Register both Debezium connectors with the local Kafka Connect worker.
# Run this after docker compose up, once Connect answers on port 8083.
set -e

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

for file in "$(dirname "$0")"/orders-connector.json "$(dirname "$0")"/inventory-connector.json; do
  name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "$file")
  echo "Registering $name"
  curl -s -X PUT \
    -H "Content-Type: application/json" \
    --data @"$file" \
    "$CONNECT_URL/connectors/$name/config" > /dev/null
done

echo "Done. Connector status follows."
curl -s "$CONNECT_URL/connectors?expand=status" | python3 -m json.tool | head -40
