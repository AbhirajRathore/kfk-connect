"""
Kafka Connect Exporter
- Polls each worker's REST API every POLL_INTERVAL_SECONDS
- Exposes Prometheus metrics on :8000/metrics
- Pings healthchecks.io when healthy (dead-man's-switch for Slack alerts)
- Pings healthchecks.io /fail endpoint when a task fails or worker is unreachable
"""

import json
import logging
import os
import re
import sys
import time
from typing import Any

import requests
from prometheus_client import Counter, Gauge, Info, start_http_server

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("connect-exporter")

# ── Config from env ───────────────────────────────────────────────────────────
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT_SECONDS", "10"))
HC_PING_TIMEOUT = int(os.environ.get("HC_PING_TIMEOUT_SECONDS", "5"))
WORKERS_FILE = os.environ.get("WORKERS_FILE", "/app/workers.json")

# ── Prometheus metrics ────────────────────────────────────────────────────────
WORKER_UP = Gauge(
    "kafka_connect_worker_up",
    "1 if the Kafka Connect worker REST API is reachable, 0 otherwise",
    ["worker", "region", "env"],
)
CONNECTOR_STATUS = Gauge(
    "kafka_connect_connector_status",
    "1 if connector state matches the label value, else 0",
    ["worker", "region", "env", "connector", "state"],
)
TASK_STATUS = Gauge(
    "kafka_connect_task_status",
    "1 if task state matches the label value, else 0",
    ["worker", "region", "env", "connector", "task_id", "state"],
)
TASK_FAILED = Gauge(
    "kafka_connect_task_failed",
    "1 if any task in this connector is currently FAILED",
    ["worker", "region", "env", "connector"],
)
CONNECTOR_COUNT = Gauge(
    "kafka_connect_connector_count",
    "Number of connectors registered on this worker",
    ["worker", "region", "env"],
)
SCRAPE_ERRORS = Counter(
    "kafka_connect_scrape_errors_total",
    "Total number of scrape errors per worker",
    ["worker"],
)
LAST_SCRAPE = Gauge(
    "kafka_connect_last_scrape_timestamp_seconds",
    "Unix timestamp of the last successful scrape",
    ["worker", "region", "env"],
)

# ── Healthchecks.io ───────────────────────────────────────────────────────────
def _hc_url_for(region: str, env: str) -> str:
    key = f"HC_PING_URL_{region.upper()}_{env.upper()}"
    return os.environ.get(key, "")


def ping_healthcheck(url: str, fail: bool = False) -> None:
    if not url:
        return
    target = f"{url.rstrip('/')}/fail" if fail else url
    try:
        requests.get(target, timeout=HC_PING_TIMEOUT)
        logger.debug("Pinged healthcheck: %s (fail=%s)", target, fail)
    except Exception as exc:
        logger.warning("healthcheck ping failed for %s: %s", target, exc)


# ── Worker polling ────────────────────────────────────────────────────────────
def poll_worker(worker: dict[str, Any]) -> None:
    name: str = worker["name"]
    base_url: str = worker["url"].rstrip("/")
    region: str = worker["region"]
    env: str = worker["env"]
    hc_url: str = _hc_url_for(region, env)

    labels = {"worker": name, "region": region, "env": env}

    # ── 1. Reach the worker ───────────────────────────────────────────────────
    try:
        resp = requests.get(f"{base_url}/connectors", timeout=CONNECT_TIMEOUT)
        resp.raise_for_status()
        connector_names: list[str] = resp.json()
    except Exception as exc:
        logger.error("[%s] Worker unreachable: %s", name, exc)
        WORKER_UP.labels(**labels).set(0)
        SCRAPE_ERRORS.labels(worker=name).inc()
        ping_healthcheck(hc_url, fail=True)
        return

    WORKER_UP.labels(**labels).set(1)
    LAST_SCRAPE.labels(**labels).set(time.time())
    CONNECTOR_COUNT.labels(**labels).set(len(connector_names))
    logger.info("[%s] Worker reachable — %d connector(s)", name, len(connector_names))

    # ── 2. Poll each connector status ─────────────────────────────────────────
    worker_healthy = True

    for connector in connector_names:
        try:
            st_resp = requests.get(
                f"{base_url}/connectors/{connector}/status",
                timeout=CONNECT_TIMEOUT,
            )
            st_resp.raise_for_status()
            status = st_resp.json()
        except Exception as exc:
            logger.error("[%s] Failed to get status for connector '%s': %s", name, connector, exc)
            SCRAPE_ERRORS.labels(worker=name).inc()
            worker_healthy = False
            continue

        # Connector-level state
        conn_state: str = status.get("connector", {}).get("state", "UNKNOWN")
        for state in ("RUNNING", "FAILED", "PAUSED", "UNASSIGNED"):
            CONNECTOR_STATUS.labels(
                **labels, connector=connector, state=state
            ).set(1 if conn_state == state else 0)

        if conn_state != "RUNNING":
            logger.warning("[%s] Connector '%s' state=%s", name, connector, conn_state)
            worker_healthy = False

        # Task-level states
        tasks = status.get("tasks", [])
        connector_has_failed_task = False
        for task in tasks:
            task_id = str(task.get("id", "0"))
            task_state: str = task.get("state", "UNKNOWN")
            trace: str = task.get("trace", "")

            for state in ("RUNNING", "FAILED", "PAUSED", "UNASSIGNED"):
                TASK_STATUS.labels(
                    **labels, connector=connector, task_id=task_id, state=state
                ).set(1 if task_state == state else 0)

            if task_state != "RUNNING":
                worker_healthy = False
                connector_has_failed_task = True
                if trace:
                    # Log the first 800 chars of the trace for easier debugging
                    short_trace = trace[:800].replace("\n", " | ")
                    logger.error(
                        "[%s] connector=%s task=%s state=%s | %s",
                        name, connector, task_id, task_state, short_trace,
                    )
                else:
                    logger.warning(
                        "[%s] connector=%s task=%s state=%s",
                        name, connector, task_id, task_state,
                    )

        TASK_FAILED.labels(**labels, connector=connector).set(
            1 if connector_has_failed_task else 0
        )

    # ── 3. Ping healthchecks.io ───────────────────────────────────────────────
    if worker_healthy:
        logger.info("[%s] All connectors healthy — pinging healthcheck", name)
        ping_healthcheck(hc_url, fail=False)
    else:
        logger.warning("[%s] Unhealthy — pinging healthcheck /fail", name)
        ping_healthcheck(hc_url, fail=True)


# ── Workers config ────────────────────────────────────────────────────────────
def load_workers() -> list[dict[str, Any]]:
    with open(WORKERS_FILE) as fh:
        data = json.load(fh)
    active = [w for w in data["workers"] if w.get("active", True)]
    logger.info("Loaded %d active worker(s) from %s", len(active), WORKERS_FILE)
    return active


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    start_http_server(8000)
    logger.info(
        "connect-exporter listening on :8000/metrics | poll_interval=%ds", POLL_INTERVAL
    )

    while True:
        workers = load_workers()
        for worker in workers:
            try:
                poll_worker(worker)
            except Exception as exc:
                logger.exception(
                    "Unexpected error polling worker '%s': %s",
                    worker.get("name", "unknown"), exc,
                )
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
