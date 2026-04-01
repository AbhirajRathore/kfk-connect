#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# stop-all.sh — Stop all Kafka Connect workers and monitoring
# Usage:
#   ./stop-all.sh              Stop workers + monitoring
#   ./stop-all.sh --workers    Stop workers only (keep monitoring running)
#   ./stop-all.sh --monitoring Stop monitoring only (keep workers running)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STOP_WORKERS=true
STOP_MONITORING=true

case "${1:-}" in
  --workers)    STOP_MONITORING=false ;;
  --monitoring) STOP_WORKERS=false ;;
esac

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log()    { printf "${GREEN}[OK]${NC}   %s\n" "$*"; }
warn()   { printf "${YELLOW}[!!]${NC}   %s\n" "$*"; }
header() { printf "\n${BOLD}${BLUE}══════ %s ══════${NC}\n\n" "$*"; }

WORKER_DIRS=(
  "nivid-dubai/inventory/dev"
  "nivid-dubai/inventory/prod"
  "nivid-antwerp/inventory/dev"
  "nivid-antwerp/inventory/prod"
  "nivid-hk/inventory/dev"
  "nivid-hk/inventory/prod"
  "nivid-ny/inventory/dev"
  "nivid-ny/inventory/prod"
  # Uncomment when India is deployed:
  # "nivid-india/inventory/dev"
  "nivid-india/inventory/prod"
)

if [[ "$STOP_WORKERS" == true ]]; then
  header "Stopping Kafka Connect Workers"
  for rel_path in "${WORKER_DIRS[@]}"; do
    dir="$SCRIPT_DIR/$rel_path"
    if [[ ! -f "$dir/docker-compose.yaml" && ! -f "$dir/docker-compose.yml" ]]; then
      continue
    fi
    name=$(basename "$(dirname "$rel_path")")_$(basename "$rel_path")
    printf "  Stopping %-40s ... " "$rel_path"
    (cd "$dir" && docker compose --env-file .env down 2>/dev/null) && \
      printf "${GREEN}done${NC}\n" || \
      printf "${RED}failed${NC}\n"
  done
fi

if [[ "$STOP_MONITORING" == true ]]; then
  header "Stopping Monitoring Stack"
  (cd "$SCRIPT_DIR/monitoring" && docker compose down 2>&1) | tail -1
  log "Monitoring stack stopped"
fi

header "Remaining Containers"
docker ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || true
echo ""
log "All done."
