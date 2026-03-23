# Kafka Connect — Quick Start / Stop

| Info | Value |
|:-----|:------|
| **Server** | `10.130.3.18` |
| **SSH** | `ssh root@10.130.3.18` |
| **Repo path** | `~/kfk-connect/` |

---

## Start

```bash
cd ~/kfk-connect
./start-all.sh
```

> Skip connector registration (e.g. after reboot — connectors auto-resume)

```bash
./start-all.sh --no-connectors
```

---

## Stop

```bash
cd ~/kfk-connect
./stop-all.sh
```

| Option | Command |
|:-------|:--------|
| Stop only workers (keep Grafana/Prometheus) | `./stop-all.sh --workers` |
| Stop only monitoring (keep workers) | `./stop-all.sh --monitoring` |

---

## Monitoring

**On your laptop** — run this SSH tunnel (leave the terminal open):

```bash
ssh -N -L 3000:localhost:3000 -L 9091:localhost:9091 -L 9093:localhost:9093 root@10.130.3.18
```

Then open in your browser:

| Tool | URL | Purpose |
|:-----|:----|:--------|
| **Grafana** | http://localhost:3000 | Dashboards, connector status, logs |
| **Prometheus** | http://localhost:9091 | Metrics, query explorer |
| **Alertmanager** | http://localhost:9093 | Active alerts |

> Grafana login: `admin` / password from `monitoring/.env` (`GRAFANA_ADMIN_PASSWORD`)

---

## systemd (if auto-start is enabled)

```bash
systemctl start kfk-connect    # start all
systemctl stop kfk-connect     # stop all
systemctl status kfk-connect   # check status
```

---

## After `git pull` on the server

1. **Pull**
   ```bash
   cd ~/kfk-connect && git pull
   ```

2. **Apply changes by what changed** (pick what applies):

   | What changed | What to run |
   |:-------------|:------------|
   | `kfk-connect.service` | `sudo cp ~/kfk-connect/kfk-connect.service /etc/systemd/system/ && sudo systemctl daemon-reload` |
   | `monitoring/docker-compose.yaml`, `monitoring/.env`, Grafana/Prometheus/Loki/Promtail configs | `cd ~/kfk-connect/monitoring && docker compose up -d` (recreates services if compose changed) |
   | `monitoring/connect-exporter/*` (e.g. `workers.json`, `exporter.py`, `Dockerfile`) | `cd ~/kfk-connect/monitoring && docker compose build connect-exporter && docker compose up -d connect-exporter` |
   | `monitoring/prometheus/*` only | `curl -X POST http://localhost:9091/-/reload` (if Prometheus started with `--web.enable-lifecycle`) |
   | Region worker `docker-compose.yaml` or `.env` | `cd ~/kfk-connect/nivid-<region>/inventory/<dev\|prod> && docker compose --env-file .env up -d` |
   | Connector JSON only (same connector name) | `curl -X PUT http://localhost:<PORT>/connectors/<name>/config -H "Content-Type: application/json" -d @path/to/config.json` (or delete + POST from SETUP.md) |
   | `start-all.sh` / `stop-all.sh` only | `chmod +x ~/kfk-connect/start-all.sh ~/kfk-connect/stop-all.sh` (if needed) |

3. **Smoke check**
   ```bash
   docker ps --format "table {{.Names}}\t{{.Status}}"
   ```

> **Tip:** If unsure, safest is `./start-all.sh --no-connectors` after pull (starts/recreates stacks without re-registering connectors), or restart only the stack you touched.
