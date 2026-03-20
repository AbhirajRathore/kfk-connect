#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# start-all.sh — Start all Kafka Connect workers, monitoring, and connectors
# Usage:
#   ./start-all.sh                  Start everything + register connectors
#   ./start-all.sh --no-connectors  Start containers only (connectors auto-resume)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAX_WAIT=300
POLL_INTERVAL=10
REGISTER_CONNECTORS=true

if [[ "${1:-}" == "--no-connectors" ]]; then
  REGISTER_CONNECTORS=false
fi

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log()    { printf "${GREEN}[OK]${NC}   %s\n" "$*"; }
warn()   { printf "${YELLOW}[!!]${NC}   %s\n" "$*"; }
err()    { printf "${RED}[ERR]${NC}  %s\n" "$*"; }
header() { printf "\n${BOLD}${BLUE}══════ %s ══════${NC}\n\n" "$*"; }

# Format: compose_dir | port | connector_name | config_json | container_name
WORKERS=(
  "nivid-dubai/inventory/dev|8084|dubai_inventory|dubai_inventory_dev.json|dubai_inventory_dev"
  "nivid-dubai/inventory/prod|8085|dubai_inventory|dubai_inventory_live.json|dubai_inventory_prod"
  "nivid-antwerp/inventory/dev|8089|antwerp_inventory|antwerp_inventory_dev.json|antwerp_inventory_dev"
  "nivid-antwerp/inventory/prod|8090|antwerp_inventory|antwerp_inventory_live.json|antwerp_inventory_prod"
  "nivid-hk/inventory/dev|8099|hk_inventory|hk_inventory_dev.json|hk_inventory_dev"
  "nivid-hk/inventory/prod|8100|hk_inventory|hk_inventory_live.json|hk_inventory_prod"
  "nivid-ny/inventory/dev|8095|ny_inventory|ny_inventory_dev.json|ny_inventory_dev"
  "nivid-ny/inventory/prod|8096|ny_inventory|ny_inventory_live.json|ny_inventory_prod"
  # Uncomment when India is deployed:
  # "nivid-india/inventory/dev|8097|india_inventory_19c|india_inventory_19c.json|india_inventory_19c"
  # "nivid-india/inventory/prod|8083|india_inventory|india_inventory_live.json|india_inventory_prod"
)

# ── Step 1: Docker network ───────────────────────────────────────────────────
header "Docker Network"
if docker network inspect monitoring_net &>/dev/null; then
  log "monitoring_net already exists"
else
  docker network create monitoring_net
  log "Created monitoring_net"
fi

# ── Step 2: Monitoring stack ─────────────────────────────────────────────────
header "Monitoring Stack"
(cd "$SCRIPT_DIR/monitoring" && docker compose up -d 2>&1) | tail -1
log "Monitoring stack started"

# ── Step 3: Worker containers ────────────────────────────────────────────────
header "Kafka Connect Workers"
for entry in "${WORKERS[@]}"; do
  IFS='|' read -r rel_path port connector_name config_json container_name <<< "$entry"
  dir="$SCRIPT_DIR/$rel_path"

  if [[ ! -f "$dir/docker-compose.yaml" && ! -f "$dir/docker-compose.yml" ]]; then
    warn "Skipping $container_name — no compose file in $rel_path"
    continue
  fi

  printf "  Starting %-30s ... " "$container_name"
  (cd "$dir" && docker compose --env-file .env up -d 2>/dev/null) && \
    printf "${GREEN}done${NC}\n" || \
    printf "${RED}failed${NC}\n"
done

# ── Step 4: Wait for workers to accept connections ───────────────────────────
header "Waiting for Workers"
printf "  Workers need ~2-3 min to install JDBC plugin and start.\n\n"

all_ready=true
for entry in "${WORKERS[@]}"; do
  IFS='|' read -r rel_path port connector_name config_json container_name <<< "$entry"
  printf "  %-30s :" "$container_name"

  elapsed=0
  while ! curl -sf "http://localhost:$port/connectors" &>/dev/null; do
    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
    if [[ $elapsed -ge $MAX_WAIT ]]; then
      printf " ${RED}TIMEOUT after ${MAX_WAIT}s${NC}\n"
      all_ready=false
      continue 2
    fi
    printf "."
  done
  printf " ${GREEN}ready (${elapsed}s)${NC}\n"
done

if [[ "$all_ready" == false ]]; then
  warn "Some workers did not start in time. Check: docker ps"
fi

# ── Step 5: Register connectors ─────────────────────────────────────────────
if [[ "$REGISTER_CONNECTORS" == true ]]; then
  header "Registering Connectors"
  printf "  (Existing connectors are skipped — they auto-resume from Kafka topics)\n\n"

  for entry in "${WORKERS[@]}"; do
    IFS='|' read -r rel_path port connector_name config_json container_name <<< "$entry"
    config_path="$SCRIPT_DIR/$rel_path/$config_json"

    if ! curl -sf "http://localhost:$port/connectors" &>/dev/null; then
      warn "$container_name (port $port) is not reachable — skipping connector"
      continue
    fi

    existing=$(curl -sf "http://localhost:$port/connectors" 2>/dev/null || echo "[]")
    if echo "$existing" | grep -q "\"$connector_name\""; then
      log "$container_name  →  $connector_name already exists"
      continue
    fi

    printf "  Registering %-22s on %-30s ... " "$connector_name" "$container_name"
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST "http://localhost:$port/connectors" \
      -H "Content-Type: application/json" \
      -d "{\"name\": \"$connector_name\", \"config\": $(cat "$config_path")}") || true

    if [[ "$http_code" == "201" ]]; then
      printf "${GREEN}created${NC}\n"
    elif [[ "$http_code" == "409" ]]; then
      printf "${YELLOW}already exists${NC}\n"
    else
      printf "${RED}failed (HTTP $http_code)${NC}\n"
    fi
  done
else
  header "Connector Registration Skipped"
  printf "  Connectors auto-resume from Kafka topics after restart.\n"
  printf "  Run without --no-connectors to register new connectors.\n"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
header "Status Summary"
echo ""
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null | head -25
echo ""
log "All done."
