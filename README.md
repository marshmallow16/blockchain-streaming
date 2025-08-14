# Blockchain Streaming (Redpanda → ClickHouse → Grafana)

Spin up a local pipeline that ingests on-chain events into ClickHouse and visualizes them in Grafana.

## Quick start
```bash
cp .env.example .env
docker compose up -d

Grafana: http://localhost:3000

ClickHouse HTTP: http://localhost:8123

Stack

Redpanda (Kafka API)

ClickHouse 24.3

Grafana 11.x with ClickHouse datasource
