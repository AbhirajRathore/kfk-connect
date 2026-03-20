# First-Time Setup Guide
## Kafka Connect — Portainer Deployment (Dev + Prod Inventory)

This guide walks through deploying all 5 dev and all 5 prod inventory Kafka Connect workers and the monitoring stack on the Portainer server.

**Server:** `10.130.3.18`
**Portainer UI:** `https://10.130.3.18:9443`
**Repo:** `https://github.com/AbhirajRathore/kfk-connect` (branch: `prometheus`)
**Repo path on server:** `~/kfk-connect/` (i.e. `/root/kfk-connect/`)

---

## Architecture at a Glance

```
Oracle DB (Dev: 10.130.3.5:1521/ORCL19C | Prod: 10.130.3.5:1521/orcl19c)
        │
        │  JDBC Source Connector (poll every 10s)
        ▼
┌─────────────────────────────────────────────────────────────┐
│              Kafka Connect Workers                          │
│  ── Dev ──────────────────────────────────────────────────  │
│  dubai:8084  antwerp:8089  india:8097  hk:8099  ny:8095    │
│  ── Prod ─────────────────────────────────────────────────  │
│  dubai:8085  antwerp:8090  india:8083  hk:8100  ny:8096    │
└─────────────────────────────────────────────────────────────┘
        │
        │  Avro + Schema Registry
        ▼
Confluent Cloud (pkc-56d1g.eastus.azure.confluent.cloud)
        Topics: dubai_inventory, antwerp_inventory, etc.

── Monitoring (separate stack) ───────────────────────────
  connect-exporter → polls all 10 worker REST APIs every 30s
  Prometheus       → stores metrics, fires alert rules
  Alertmanager     → sends email via Gmail SMTP
  healthchecks.io  → fires Slack when ping stops
  Loki + Promtail  → collects all container logs
  Grafana          → dashboards + log explorer
  cAdvisor         → container CPU / memory
```

---

## Prerequisites Checklist

Before starting, confirm:

- [ ] SSH access to `10.130.3.18` as `root`
- [ ] Docker is running on the server (`docker ps` works)
- [ ] Server can reach Oracle DB at `10.130.3.5:1521`
- [ ] Server can reach Confluent Cloud (outbound 9092, 443)
- [ ] You have a [healthchecks.io](https://healthchecks.io) account (for Slack alerts)
- [ ] You have a recipient email address for alerts

---

## Part 1 — On the Portainer Server

### 1.1 — SSH into the server

```bash
ssh root@10.130.3.18
```

---

### 1.2 — Clone the repository

```bash
cd /opt
git clone -b prometheus https://github.com/AbhirajRathore/kfk-connect.git
cd kfk-connect
```

Verify the structure is there:
```bash
ls monitoring/
ls nivid-dubai/inventory/dev/
```

---

### 1.3 — Set up healthchecks.io checks (for Slack alerts)

> Skip this section if you want to set up Slack alerts later. Email alerts will still work.

1. Go to [https://healthchecks.io](https://healthchecks.io) → log in
2. Create **5 new checks** — one per location:

| Check name | Period | Grace |
|---|---|---|
| `kfk-connect-dubai-dev` | 1 minute | 2 minutes |
| `kfk-connect-antwerp-dev` | 1 minute | 2 minutes |
| `kfk-connect-india-dev` | 1 minute | 2 minutes |
| `kfk-connect-hk-dev` | 1 minute | 2 minutes |
| `kfk-connect-ny-dev` | 1 minute | 2 minutes |

3. In your project → **Integrations** → connect your Slack workspace
4. Copy the ping URL for each check (format: `https://hc-ping.com/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

---

### 1.4 — Configure the monitoring `.env`

```bash
nano ~/kfk-connect/monitoring/.env
```

Fill in the 6 placeholder values:

```bash
# Set a strong Grafana login password
GRAFANA_ADMIN_PASSWORD=YourStrongPasswordHere

# Email address that receives all alert emails
ALERT_EMAIL_TO=your.name@example.com

# Paste ping URLs from healthchecks.io (Step 1.3)
HC_PING_URL_DUBAI_DEV=https://hc-ping.com/YOUR-UUID-DUBAI
HC_PING_URL_ANTWERP_DEV=https://hc-ping.com/YOUR-UUID-ANTWERP
HC_PING_URL_INDIA_DEV=https://hc-ping.com/YOUR-UUID-INDIA
HC_PING_URL_HK_DEV=https://hc-ping.com/YOUR-UUID-HK
HC_PING_URL_NY_DEV=https://hc-ping.com/YOUR-UUID-NY
```

Save: `Ctrl+O` → Enter → `Ctrl+X`

Verify the file looks correct:
```bash
cat ~/kfk-connect/monitoring/.env
```

---

### 1.5 — Create the shared Docker network

```bash
docker network create monitoring_net
```

This network connects the monitoring stack to all worker containers. Run once — it persists across reboots.

---

### 1.6 — Deploy the monitoring stack

```bash
cd ~/kfk-connect/monitoring
docker compose up -d
```

The `connect-exporter` image will build from source (~2 minutes first time).

Check all 7 monitoring containers are running:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep monitoring
```

Expected output:
```
monitoring_prometheus        Up X minutes
monitoring_alertmanager      Up X minutes
monitoring_grafana           Up X minutes
monitoring_loki              Up X minutes
monitoring_promtail          Up X minutes
monitoring_cadvisor          Up X minutes
monitoring_connect_exporter  Up X minutes
```

If any container shows `Restarting` or `Exited`, check logs:
```bash
docker logs monitoring_connect_exporter --tail=50
docker logs monitoring_prometheus --tail=50
```

---

## Part 2 — Deploy the 5 Dev Inventory Workers

Each worker runs as a separate Docker Compose stack. Deploy them one at a time and verify before moving to the next.

---

### 2.1 — Dubai Inventory Dev (port 8084)

```bash
cd ~/kfk-connect/nivid-dubai/inventory/dev
docker compose --env-file .env up -d
```

Wait ~3 minutes for the JDBC connector plugin to install, then check:
```bash
docker logs dubai_inventory_dev --tail=20
```

Look for: `Kafka Connect started` — this means the worker is ready.

Register the connector:
```bash
curl -s -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dubai_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-dubai/inventory/dev/dubai_inventory_dev.json)"'
  }' | python3 -m json.tool
```

Verify it is running:
```bash
curl -s http://localhost:8084/connectors/dubai_inventory/status | python3 -m json.tool
```

Expected: `"state": "RUNNING"` for both connector and task.

---

### 2.2 — Antwerp Inventory Dev (port 8089)

```bash
cd ~/kfk-connect/nivid-antwerp/inventory/dev
docker compose --env-file .env up -d
```

Wait for startup, then register connector:
```bash
curl -s -X POST http://localhost:8089/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "antwerp_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-antwerp/inventory/dev/antwerp_inventory_dev.json)"'
  }' | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8089/connectors/antwerp_inventory/status | python3 -m json.tool
```

---

### 2.3 — India Inventory Dev (port 8097)

```bash
cd ~/kfk-connect/nivid-india/inventory/dev
docker compose --env-file .env up -d
```

Register connector:
```bash
curl -s -X POST http://localhost:8097/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "india_inventory_19c",
    "config": '"$(cat ~/kfk-connect/nivid-india/inventory/dev/india_inventory_19c.json)"'
  }' | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8097/connectors/india_inventory_19c/status | python3 -m json.tool
```

---

### 2.4 — HK Inventory Dev (port 8099)

```bash
cd ~/kfk-connect/nivid-hk/inventory/dev
docker compose --env-file .env up -d
```

Register connector:
```bash
curl -s -X POST http://localhost:8099/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hk_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-hk/inventory/dev/hk_inventory_dev.json)"'
  }' | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8099/connectors/hk_inventory/status | python3 -m json.tool
```

---

### 2.5 — NY Inventory Dev (port 8095)

```bash
cd ~/kfk-connect/nivid-ny/inventory/dev
docker compose --env-file .env up -d
```

Register connector:
```bash
curl -s -X POST http://localhost:8095/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ny_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-ny/inventory/dev/ny_inventory_dev.json)"'
  }' | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8095/connectors/ny_inventory/status | python3 -m json.tool
```

---

## Part 3 — Final Verification

### 3.1 — Check all containers are up

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

You should see 12 containers total: 7 monitoring + 5 workers.

---

### 3.2 — Check all connectors at once

```bash
for port in 8084 8089 8097 8099 8095; do
  echo ""
  echo "════ Port $port ════"
  curl -sf http://localhost:$port/connectors
done
```

---

### 3.3 — Check the connect-exporter is detecting everything

```bash
docker logs monitoring_connect_exporter --tail=50
```

You should see lines like:
```
[dubai_inventory_dev] Worker reachable — 1 connector(s)
[dubai_inventory_dev] All connectors healthy — pinging healthcheck
[antwerp_inventory_dev] Worker reachable — 1 connector(s)
...
```

---

### 3.4 — Check Prometheus has the metrics

```bash
curl -s "http://localhost:9091/api/v1/query?query=kafka_connect_worker_up" \
  | python3 -m json.tool
```

All 5 workers should show `"value": "1"`.

---

### 3.5 — Send a test alert email

```bash
curl -s -X POST http://localhost:9093/api/v2/alerts \
  -H "Content-Type: application/json" \
  -d '[{
    "labels": {"alertname":"TestAlert","severity":"warning","region":"dubai","env":"dev"},
    "annotations": {"summary":"Test alert — setup verification","description":"This is a test. If you received this email the alerting pipeline is working correctly."}
  }]'
```

Check your inbox at `ALERT_EMAIL_TO`. The alert resolves automatically after ~5 minutes.

---

## Part 4 — Access Grafana from Your Laptop

Run this **on your laptop** (not the server). Leave the terminal open:

```bash
ssh -N \
  -L 3000:localhost:3000 \
  -L 9091:localhost:9091 \
  -L 9093:localhost:9093 \
  root@10.130.3.18
```

Open in browser:

| URL | What you see |
|---|---|
| `http://localhost:3000` | Grafana (login: admin / your password) |
| `http://localhost:9091` | Prometheus query explorer |
| `http://localhost:9093` | Alertmanager active alerts |

In Grafana → **Dashboards** → **Kafka Connect — Overview** — you will see all 5 workers, connector states, and live error logs.

---

## Troubleshooting

### Worker container starts but connector stays FAILED

```bash
docker logs dubai_inventory_dev --tail=100 | grep -E "ERROR|WARN|ORA-|Exception"
```

Common causes:
- Oracle DB not reachable from server → `telnet 10.130.3.5 1521`
- Wrong DB credentials in the JSON connector config
- Confluent Cloud auth failed → check `.env` SASL credentials

### Connector shows UNASSIGNED after POST

Worker is still starting up. Wait 60 seconds and check again:
```bash
curl -s http://localhost:8084/connectors/dubai_inventory/status | python3 -m json.tool
```

### connect-exporter shows "Worker unreachable"

The worker container is on `monitoring_net` but the exporter cannot reach it. Verify the worker joined the network:
```bash
docker inspect dubai_inventory_dev | grep -A5 '"monitoring_net"'
```

If missing, the worker stack needs to be redeployed (the updated `docker-compose.yaml` adds it automatically).

### Email alerts not arriving

```bash
docker logs monitoring_alertmanager --tail=50
```

Check for SMTP errors. Verify the Gmail app password in `monitoring/.env` is correct (format: `xxxx xxxx xxxx xxxx` with spaces).

---

## Part 5 — Deploy the 5 Prod Inventory Workers

> **Pre-requisite:** All 5 dev workers are running and healthy (Part 2 complete). Dev containers must remain up — prod workers join the same `monitoring_net` network and run alongside them.

Each prod worker runs on a different port from its dev counterpart. Deploy one at a time and verify before moving to the next.

---

### 5.0 — Set up healthchecks.io prod checks (for Slack alerts)

> Skip if you want to set up Slack alerts later. Email alerts will still work.

1. Go to [https://healthchecks.io](https://healthchecks.io) → log in
2. Create **5 new checks** — one per prod location:

| Check name | Period | Grace |
|---|---|---|
| `kfk-connect-dubai-prod` | 1 minute | 2 minutes |
| `kfk-connect-antwerp-prod` | 1 minute | 2 minutes |
| `kfk-connect-india-prod` | 1 minute | 2 minutes |
| `kfk-connect-hk-prod` | 1 minute | 2 minutes |
| `kfk-connect-ny-prod` | 1 minute | 2 minutes |

3. Copy the ping URL for each check (format: `https://hc-ping.com/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

---

### 5.1 — Add prod ping URLs to monitoring `.env`

```bash
nano ~/kfk-connect/monitoring/.env
```

Uncomment and fill in the Phase 2 lines:

```bash
HC_PING_URL_DUBAI_PROD=https://hc-ping.com/YOUR-UUID-DUBAI-PROD
HC_PING_URL_ANTWERP_PROD=https://hc-ping.com/YOUR-UUID-ANTWERP-PROD
HC_PING_URL_INDIA_PROD=https://hc-ping.com/YOUR-UUID-INDIA-PROD
HC_PING_URL_HK_PROD=https://hc-ping.com/YOUR-UUID-HK-PROD
HC_PING_URL_NY_PROD=https://hc-ping.com/YOUR-UUID-NY-PROD
```

Save: `Ctrl+O` → Enter → `Ctrl+X`

---

### 5.2 — Dubai Inventory Prod (port 8085)

**Create the `.env` file** (same Confluent Cloud credentials as dev):

```bash
cp ~/kfk-connect/nivid-dubai/inventory/dev/.env \
   ~/kfk-connect/nivid-dubai/inventory/prod/.env
```

**Deploy the worker:**

```bash
cd ~/kfk-connect/nivid-dubai/inventory/prod
docker compose --env-file .env up -d
```

Wait ~3 minutes for the JDBC connector plugin to install, then check:

```bash
docker logs dubai_inventory_prod --tail=20
```

Look for: `Kafka Connect started`

**Register the connector:**

```bash
curl -s -X POST http://localhost:8085/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dubai_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-dubai/inventory/prod/dubai_inventory_live.json)"'
  }' | python3 -m json.tool
```

**Verify it is running:**

```bash
curl -s http://localhost:8085/connectors/dubai_inventory/status | python3 -m json.tool
```

Expected: `"state": "RUNNING"` for both connector and task.

---

### 5.3 — Antwerp Inventory Prod (port 8090)

```bash
cp ~/kfk-connect/nivid-antwerp/inventory/dev/.env \
   ~/kfk-connect/nivid-antwerp/inventory/prod/.env

cd ~/kfk-connect/nivid-antwerp/inventory/prod
docker compose --env-file .env up -d
```

Wait for startup, then register connector:

```bash
curl -s -X POST http://localhost:8090/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "antwerp_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-antwerp/inventory/prod/antwerp_inventory_live.json)"'
  }' | python3 -m json.tool
```

Verify:

```bash
curl -s http://localhost:8090/connectors/antwerp_inventory/status | python3 -m json.tool
```

---

### 5.4 — India Inventory Prod (port 8083)

```bash
cp ~/kfk-connect/nivid-india/inventory/dev/.env \
   ~/kfk-connect/nivid-india/inventory/prod/.env

cd ~/kfk-connect/nivid-india/inventory/prod
docker compose --env-file .env up -d
```

Register connector:

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "india_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-india/inventory/prod/india_inventory_live.json)"'
  }' | python3 -m json.tool
```

Verify:

```bash
curl -s http://localhost:8083/connectors/india_inventory/status | python3 -m json.tool
```

---

### 5.5 — HK Inventory Prod (port 8100)

```bash
cp ~/kfk-connect/nivid-hk/inventory/dev/.env \
   ~/kfk-connect/nivid-hk/inventory/prod/.env

cd ~/kfk-connect/nivid-hk/inventory/prod
docker compose --env-file .env up -d
```

Register connector:

```bash
curl -s -X POST http://localhost:8100/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hk_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-hk/inventory/prod/hk_inventory_live.json)"'
  }' | python3 -m json.tool
```

Verify:

```bash
curl -s http://localhost:8100/connectors/hk_inventory/status | python3 -m json.tool
```

---

### 5.6 — NY Inventory Prod (port 8096)

```bash
cp ~/kfk-connect/nivid-ny/inventory/dev/.env \
   ~/kfk-connect/nivid-ny/inventory/prod/.env

cd ~/kfk-connect/nivid-ny/inventory/prod
docker compose --env-file .env up -d
```

Register connector:

```bash
curl -s -X POST http://localhost:8096/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ny_inventory",
    "config": '"$(cat ~/kfk-connect/nivid-ny/inventory/prod/ny_inventory_live.json)"'
  }' | python3 -m json.tool
```

Verify:

```bash
curl -s http://localhost:8096/connectors/ny_inventory/status | python3 -m json.tool
```

---

## Part 6 — Activate Prod Workers in Monitoring

Once all 5 prod workers are running, enable them in the connect-exporter so Prometheus starts scraping them and healthchecks.io receives pings.

### 6.1 — Enable prod workers in `workers.json`

```bash
nano ~/kfk-connect/monitoring/connect-exporter/workers.json
```

For each of the 5 prod worker entries, change `"active": false` → `"active": true`:

```json
{ "name": "dubai_inventory_prod",   "url": "http://dubai_inventory_prod:8085",   "region": "dubai",   "env": "prod", "active": true },
{ "name": "antwerp_inventory_prod", "url": "http://antwerp_inventory_prod:8090", "region": "antwerp", "env": "prod", "active": true },
{ "name": "india_inventory_prod",   "url": "http://india_inventory_prod:8083",   "region": "india",   "env": "prod", "active": true },
{ "name": "hk_inventory_prod",      "url": "http://hk_inventory_prod:8100",      "region": "hk",      "env": "prod", "active": true },
{ "name": "ny_inventory_prod",      "url": "http://ny_inventory_prod:8096",      "region": "ny",      "env": "prod", "active": true }
```

Save: `Ctrl+O` → Enter → `Ctrl+X`

---

### 6.2 — Rebuild and restart the connect-exporter

```bash
cd ~/kfk-connect/monitoring
docker compose build connect-exporter
docker compose up -d connect-exporter
```

---

### 6.3 — Verify the exporter sees all 10 workers

```bash
docker logs monitoring_connect_exporter --tail=60
```

You should see lines for all 10 workers — 5 dev + 5 prod:

```
[dubai_inventory_dev]    Worker reachable — 1 connector(s)
[dubai_inventory_prod]   Worker reachable — 1 connector(s)
[antwerp_inventory_dev]  Worker reachable — 1 connector(s)
[antwerp_inventory_prod] Worker reachable — 1 connector(s)
...
```

---

### 6.4 — Check all 10 containers are up

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

You should see **17 containers total**: 7 monitoring + 5 dev workers + 5 prod workers.

---

### 6.5 — Spot-check all prod connectors at once

```bash
for port in 8085 8090 8083 8100 8096; do
  echo ""
  echo "════ Port $port ════"
  curl -sf http://localhost:$port/connectors
done
```

---

### 6.6 — Send a prod test alert email

```bash
curl -s -X POST http://localhost:9093/api/v2/alerts \
  -H "Content-Type: application/json" \
  -d '[{
    "labels": {"alertname":"TestAlert","severity":"warning","region":"dubai","env":"prod"},
    "annotations": {"summary":"Test alert — prod setup verification","description":"This is a test. If you received this email the prod alerting pipeline is working correctly."}
  }]'
```

---

## Quick Reference — Full Port Map (Dev + Prod)

| Container | Env | Port | REST endpoint |
|---|---|---|---|
| `dubai_inventory_dev` | dev | 8084 | `http://localhost:8084/connectors` |
| `dubai_inventory_prod` | prod | 8085 | `http://localhost:8085/connectors` |
| `antwerp_inventory_dev` | dev | 8089 | `http://localhost:8089/connectors` |
| `antwerp_inventory_prod` | prod | 8090 | `http://localhost:8090/connectors` |
| `india_inventory_19c` | dev | 8097 | `http://localhost:8097/connectors` |
| `india_inventory_prod` | prod | 8083 | `http://localhost:8083/connectors` |
| `hk_inventory_dev` | dev | 8099 | `http://localhost:8099/connectors` |
| `hk_inventory_prod` | prod | 8100 | `http://localhost:8100/connectors` |
| `ny_inventory_dev` | dev | 8095 | `http://localhost:8095/connectors` |
| `ny_inventory_prod` | prod | 8096 | `http://localhost:8096/connectors` |
| Grafana | — | 3000 | `http://localhost:3000` |
| Prometheus | — | 9091 | `http://localhost:9091` |
| Alertmanager | — | 9093 | `http://localhost:9093` |

---

## Quick Reference — Connector Name per Worker (Dev + Prod)

| Worker | Container | Connector name |
|---|---|---|
| Dubai dev | `dubai_inventory_dev` | `dubai_inventory` |
| Dubai prod | `dubai_inventory_prod` | `dubai_inventory` |
| Antwerp dev | `antwerp_inventory_dev` | `antwerp_inventory` |
| Antwerp prod | `antwerp_inventory_prod` | `antwerp_inventory` |
| India dev | `india_inventory_19c` | `india_inventory_19c` |
| India prod | `india_inventory_prod` | `india_inventory` |
| HK dev | `hk_inventory_dev` | `hk_inventory` |
| HK prod | `hk_inventory_prod` | `hk_inventory` |
| NY dev | `ny_inventory_dev` | `ny_inventory` |
| NY prod | `ny_inventory_prod` | `ny_inventory` |

Restart a failed task:
```bash
curl -X POST http://localhost:<PORT>/connectors/<connector-name>/tasks/0/restart
```
