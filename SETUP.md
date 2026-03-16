# First-Time Setup Guide
## Kafka Connect — Portainer Deployment (Dev Inventory)

This guide walks through deploying all 5 dev inventory Kafka Connect workers and the monitoring stack on the Portainer server for the first time.

**Server:** `10.130.3.18`
**Portainer UI:** `https://10.130.3.18:9443`
**Repo:** `https://github.com/AbhirajRathore/kfk-connect` (branch: `prometheus`)

---

## Architecture at a Glance

```
Oracle DB (10.130.3.5:1521/ORCL19C)
        │
        │  JDBC Source Connector (poll every 10s)
        ▼
┌─────────────────────────────────────────┐
│         Kafka Connect Workers           │
│  dubai:8084  antwerp:8089  india:8097   │
│  hk:8099     ny:8095                   │
└─────────────────────────────────────────┘
        │
        │  Avro + Schema Registry
        ▼
Confluent Cloud (pkc-56d1g.eastus.azure.confluent.cloud)
        Topics: dubai_inventory, antwerp_inventory, etc.

── Monitoring (separate stack) ───────────────────────────
  connect-exporter → polls worker REST APIs every 30s
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
nano /opt/kfk-connect/monitoring/.env
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
cat /opt/kfk-connect/monitoring/.env
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
cd /opt/kfk-connect/monitoring
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
cd /opt/kfk-connect/nivid-dubai/inventory/dev
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
  -d @/opt/kfk-connect/nivid-dubai/inventory/dev/dubai_inventory_dev.json \
  | python3 -m json.tool
```

Verify it is running:
```bash
curl -s http://localhost:8084/connectors/dubai_inventory/status | python3 -m json.tool
```

Expected: `"state": "RUNNING"` for both connector and task.

---

### 2.2 — Antwerp Inventory Dev (port 8089)

```bash
cd /opt/kfk-connect/nivid-antwerp/inventory/dev
docker compose --env-file .env up -d
```

Wait for startup, then register connector:
```bash
curl -s -X POST http://localhost:8089/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kfk-connect/nivid-antwerp/inventory/dev/antwerp_inventory_dev.json \
  | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8089/connectors/antwerp_inventory/status | python3 -m json.tool
```

---

### 2.3 — India Inventory Dev (port 8097)

```bash
cd /opt/kfk-connect/nivid-india/inventory/dev
docker compose --env-file .env up -d
```

Register connector:
```bash
curl -s -X POST http://localhost:8097/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kfk-connect/nivid-india/inventory/dev/india_inventory_19c.json \
  | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8097/connectors/india_inventory_19c/status | python3 -m json.tool
```

---

### 2.4 — HK Inventory Dev (port 8099)

```bash
cd /opt/kfk-connect/nivid-hk/inventory/dev
docker compose --env-file .env up -d
```

Register connector:
```bash
curl -s -X POST http://localhost:8099/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kfk-connect/nivid-hk/inventory/dev/hk_inventory_dev.json \
  | python3 -m json.tool
```

Verify:
```bash
curl -s http://localhost:8099/connectors/hk_inventory/status | python3 -m json.tool
```

---

### 2.5 — NY Inventory Dev (port 8095)

```bash
cd /opt/kfk-connect/nivid-ny/inventory/dev
docker compose --env-file .env up -d
```

Register connector:
```bash
curl -s -X POST http://localhost:8095/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kfk-connect/nivid-ny/inventory/dev/ny_inventory_dev.json \
  | python3 -m json.tool
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
curl -s "http://localhost:9090/api/v1/query?query=kafka_connect_worker_up" \
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
  -L 9090:localhost:9090 \
  -L 9093:localhost:9093 \
  root@10.130.3.18
```

Open in browser:

| URL | What you see |
|---|---|
| `http://localhost:3000` | Grafana (login: admin / your password) |
| `http://localhost:9090` | Prometheus query explorer |
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

## Quick Reference — Port Map

| Container | Port | REST endpoint |
|---|---|---|
| `dubai_inventory_dev` | 8084 | `http://localhost:8084/connectors` |
| `antwerp_inventory_dev` | 8089 | `http://localhost:8089/connectors` |
| `india_inventory_19c` | 8097 | `http://localhost:8097/connectors` |
| `hk_inventory_dev` | 8099 | `http://localhost:8099/connectors` |
| `ny_inventory_dev` | 8095 | `http://localhost:8095/connectors` |
| Grafana | 3000 | `http://localhost:3000` |
| Prometheus | 9090 | `http://localhost:9090` |
| Alertmanager | 9093 | `http://localhost:9093` |

---

## Quick Reference — Connector Name per Worker

These are the connector names used in the REST API paths:

| Worker | Connector name |
|---|---|
| Dubai | `dubai_inventory` |
| Antwerp | `antwerp_inventory` |
| India | `india_inventory_19c` |
| HK | `hk_inventory` |
| NY | `ny_inventory` |

Restart a failed task:
```bash
curl -X POST http://localhost:<PORT>/connectors/<connector-name>/tasks/0/restart
```

---

## What to Do Next — Phase 2 (Production)

When ready to go live with production workers, see `monitoring/README.md` → **Phase 2** section.
