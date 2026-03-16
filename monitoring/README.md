# Kafka Connect Monitoring Stack

Production-grade observability for all Kafka Connect inventory workers.

## Stack Components

| Service | Port | Purpose |
|---|---|---|
| Grafana | 3000 | Dashboards, log explorer |
| Prometheus | 9091 | Metrics storage + alerting rules |
| Alertmanager | 9093 | Email alert routing (Gmail SMTP) |
| Loki | 3100 | Log aggregation (internal) |
| Promtail | — | Docker log collector → Loki |
| cAdvisor | 8081 | Container CPU / memory metrics |
| connect-exporter | — | Kafka Connect REST poller + healthchecks.io pinger |

## Alert Channels

- **Slack** → via [healthchecks.io](https://healthchecks.io) dead-man's-switch (same pattern as the existing Node.js monitoring script). The `connect-exporter` pings healthchecks.io every 30s when a worker is healthy. If pings stop, healthchecks.io fires the Slack alert.
- **Email** → Alertmanager sends directly via Gmail SMTP to `ALERT_EMAIL_TO`.

---

## First-time Setup

### Step 1 — Create the shared Docker network

```bash
docker network create monitoring_net
```

This network is shared between the monitoring stack and all worker stacks.

### Step 2 — Configure environment variables

```bash
cp monitoring/.env monitoring/.env.local  # optional backup
nano monitoring/.env
```

Fill in all values marked `FILL_IN_*`:

| Variable | What to put |
|---|---|
| `GRAFANA_ADMIN_PASSWORD` | A strong password for Grafana login |
| `ALERT_EMAIL_TO` | Email address to receive all alerts |
| `HC_PING_URL_*_DEV` | Ping URLs from healthchecks.io (see Step 3) |

### Step 3 — Set up healthchecks.io checks (for Slack)

1. Go to [https://healthchecks.io](https://healthchecks.io) and log in to your existing account
2. Create **5 new checks**, one per location:
   - `kfk-connect-dubai-dev`
   - `kfk-connect-antwerp-dev`
   - `kfk-connect-india-dev`
   - `kfk-connect-hk-dev`
   - `kfk-connect-ny-dev`
3. For each check, set:
   - **Period**: `1 minute`
   - **Grace**: `2 minutes`
4. In your healthchecks.io project **Integrations**, connect your Slack workspace (if not already done)
5. Copy the ping URL for each check into `monitoring/.env`

### Step 4 — Deploy the monitoring stack via Portainer

In Portainer → **Stacks** → **Add stack**:
- Name: `kfk-connect-monitoring`
- Build method: **Repository** or **Upload** pointing to `monitoring/docker-compose.yaml`
- Set the env vars from `monitoring/.env` in Portainer's environment editor
- Click **Deploy the stack**

Or with Docker Compose directly:
```bash
cd monitoring/
docker-compose up -d
```

### Step 5 — Deploy worker stacks via Portainer

Each worker stack has been updated with:
- `healthcheck` (polls `/connectors` endpoint)
- `monitoring_net` network
- `json-file` logging (Promtail reads these)

Deploy each worker stack in Portainer. Set the `.env` variables from the worker's `.env` file in Portainer's environment editor.

**Phase 1 workers to deploy first:**

| Stack name | Directory |
|---|---|
| `dubai-inventory-dev` | `nivid-dubai/inventory/dev/` |
| `antwerp-inventory-dev` | `nivid-antwerp/inventory/dev/` |
| `india-inventory-dev` | `nivid-india/inventory/dev/` |
| `hk-inventory-dev` | `nivid-hk/inventory/dev/` |
| `ny-inventory-dev` | `nivid-ny/inventory/dev/` |

### Step 6 — Access Grafana

- URL: `http://<your-host>:3000`
- User: `admin`
- Password: the `GRAFANA_ADMIN_PASSWORD` you set in `.env`

The **Kafka Connect — Overview** dashboard is auto-provisioned.

---

## Phase 2 — Enabling Production Workers

When you're ready to deploy prod inventory workers:

1. **Deploy prod worker stacks** in Portainer (use `*/inventory/prod/` directories)
2. **Create 5 more healthchecks.io checks** (one per prod location) and add their ping URLs to `monitoring/.env`
3. **Uncomment Phase 2 env vars** in `monitoring/.env` and `monitoring/docker-compose.yaml`
4. **Enable prod workers** in `monitoring/connect-exporter/workers.json` — set `"active": true` for all prod entries
5. **Redeploy** the monitoring stack in Portainer

---

## SSH Access

### Connecting to the Portainer host

```bash
ssh <user>@<host-ip>
```

If using a key pair:
```bash
ssh -i ~/.ssh/your_key.pem <user>@<host-ip>
```

To keep the session alive over slow connections, add to your local `~/.ssh/config`:
```
Host portainer-host
    HostName <host-ip>
    User <user>
    IdentityFile ~/.ssh/your_key.pem
    ServerAliveInterval 60
    ServerAliveCountMax 5
```

Then connect with just `ssh portainer-host`.

---

### SSH tunnels — access Grafana and Prometheus from your local browser

Because Grafana (3000), Prometheus (9091), and Alertmanager (9093) are bound to the server's loopback interface, you can forward them securely over SSH without opening those ports publicly.

**Forward all monitoring ports in a single command:**
```bash
ssh -N \
  -L 3000:localhost:3000 \
  -L 9091:localhost:9091 \
  -L 9093:localhost:9093 \
  -L 8081:localhost:8081 \
  <user>@<host-ip>
```

Then open in your browser:
- Grafana → `http://localhost:3000`
- Prometheus → `http://localhost:9091`
- Alertmanager → `http://localhost:9093`
- cAdvisor → `http://localhost:8081`

**To run the tunnel as a background process:**
```bash
ssh -fN \
  -L 3000:localhost:3000 \
  -L 9091:localhost:9091 \
  -L 9093:localhost:9093 \
  <user>@<host-ip>
```

**Or add it as a named alias in `~/.ssh/config`:**
```
Host portainer-tunnel
    HostName <host-ip>
    User <user>
    IdentityFile ~/.ssh/your_key.pem
    LocalForward 3000 localhost:3000
    LocalForward 9091 localhost:9091
    LocalForward 9093 localhost:9093
    LocalForward 8081 localhost:8081
    ServerAliveInterval 60
```

Then: `ssh -N portainer-tunnel`

---

### Common SSH + Docker commands once connected

```bash
# See all running containers (monitoring + workers)
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Tail live logs for a specific worker
docker logs dubai_inventory_dev -f --tail=100

# Tail monitoring stack logs
docker logs monitoring_connect_exporter -f --tail=100
docker logs monitoring_prometheus -f --tail=50

# Check container health status
docker inspect --format='{{.Name}} → {{.State.Health.Status}}' \
  dubai_inventory_dev antwerp_inventory_dev india_inventory_19c hk_inventory_dev ny_inventory_dev

# Restart a worker container
docker restart dubai_inventory_dev

# Pull updated images and redeploy a worker (run from the stack directory)
cd ~/kfk-connect/nivid-dubai/inventory/dev
source .env && docker-compose pull && docker-compose up -d

# Rebuild and redeploy the connect-exporter after changes
cd ~/kfk-connect/monitoring
docker-compose build connect-exporter && docker-compose up -d connect-exporter
```

---

## Useful Commands

```bash
# Check all worker statuses
for port in 8084 8089 8097 8099 8095; do
  echo "=== Port $port ===" && curl -sf http://localhost:$port/connectors | python3 -m json.tool
done

# Restart a failed task
curl -X POST http://localhost:<PORT>/connectors/<connector-name>/tasks/0/restart

# Pause / Resume a connector
curl -X PUT http://localhost:<PORT>/connectors/<connector-name>/pause
curl -X PUT http://localhost:<PORT>/connectors/<connector-name>/resume

# View connect-exporter logs
docker logs monitoring_connect_exporter -f --tail=100

# Reload Prometheus config without restart
curl -X POST http://localhost:9091/-/reload

# Check Alertmanager alerts
curl http://localhost:9093/api/v2/alerts | python3 -m json.tool
```

---

## Directory Structure

```
monitoring/
├── docker-compose.yaml                   # Full monitoring stack
├── .env                                  # Secrets (fill in before deploying)
├── README.md                             # This file
├── connect-exporter/
│   ├── exporter.py                       # Polls REST API + pings healthchecks.io
│   ├── workers.json                      # Worker list (Phase 1 active, Phase 2 commented)
│   ├── Dockerfile
│   └── requirements.txt
├── prometheus/
│   ├── prometheus.yml                    # Scrape configs
│   └── rules/
│       └── alerts.yml                    # Alert rules (worker down, task failed, etc.)
├── alertmanager/
│   ├── alertmanager.yml.tmpl             # Config template (env vars substituted at startup)
│   └── templates/
│       └── email.tmpl                    # HTML email template
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/datasources.yml   # Prometheus + Loki auto-provisioned
│   │   └── dashboards/dashboards.yml     # Dashboard folder config
│   └── dashboards/
│       └── kafka-connect-overview.json   # Main dashboard
├── loki/
│   └── loki-config.yaml
└── promtail/
    └── promtail-config.yaml
```
