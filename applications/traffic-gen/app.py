"""
============================================================
Traffic Generator — API-Controlled Load Testing Service
============================================================
Flask API (port 5003) — Default: IDLE (no traffic)
User controls start/stop/scenario/rate/duration via REST API.

Endpoints:
  GET  /status  — current state + live stats
  POST /start   — begin sending traffic
  POST /stop    — stop traffic
  GET  /health  — simple health check
============================================================
"""

import time
import random
import json
import threading
import requests
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

API_GATEWAY = "http://api-gateway:5000"
NOTIFICATION_WORKER = "http://notification-worker:5004"
INVENTORY_WORKER = "http://inventory-worker:5005"

# ============================================================
# State
# ============================================================
state = {
    "running": False,
    "scenario": None,
    "rate": 1,          # requests per second
    "duration": 60,     # seconds
    "started_at": None,
    "stats": {
        "total": 0,
        "success": 0,
        "errors": 0,
        "elapsed": 0,
    },
}
lock = threading.Lock()
stop_event = threading.Event()


# ============================================================
# Traffic Flow Functions
# ============================================================
def flow_browse_products():
    """User browses product catalog"""
    resp = requests.get(f"{API_GATEWAY}/products", timeout=10)
    return resp.status_code < 500


def flow_create_order():
    """User creates an order"""
    product_id = random.randint(1, 5)
    quantity = random.randint(1, 3)
    payload = {"product_id": product_id, "quantity": quantity}
    resp = requests.post(
        f"{API_GATEWAY}/order",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    return resp.status_code < 500


def flow_view_orders():
    """User views order history"""
    resp = requests.get(f"{API_GATEWAY}/orders?limit=10", timeout=10)
    return resp.status_code < 500


def flow_browse_then_buy():
    """User browses products, then buys one"""
    resp = requests.get(f"{API_GATEWAY}/products", timeout=10)
    if resp.status_code < 500:
        products = resp.json().get("products", [])
        if products:
            time.sleep(random.uniform(0.2, 0.8))
            product = random.choice(products)
            payload = {"product_id": product["id"], "quantity": random.randint(1, 3)}
            resp2 = requests.post(
                f"{API_GATEWAY}/order",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            return resp2.status_code < 500
    return resp.status_code < 500


def flow_check_notifications():
    """Check recent notifications from notification-worker"""
    resp = requests.get(f"{NOTIFICATION_WORKER}/notifications?limit=10", timeout=10)
    return resp.status_code < 500


def flow_check_inventory():
    """Check inventory status from inventory-worker"""
    resp = requests.get(f"{INVENTORY_WORKER}/inventory", timeout=10)
    return resp.status_code < 500


def flow_order_then_check_events():
    """Create order, then verify event processing by workers"""
    # Step 1: Create order
    product_id = random.randint(1, 5)
    payload = {"product_id": product_id, "quantity": 1}
    resp = requests.post(
        f"{API_GATEWAY}/order",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 500:
        return False
    # Step 2: Brief wait for async processing
    time.sleep(random.uniform(0.5, 1.5))
    # Step 3: Check notification-worker processed it
    resp2 = requests.get(f"{NOTIFICATION_WORKER}/status", timeout=5)
    # Step 4: Check inventory-worker processed it
    resp3 = requests.get(f"{INVENTORY_WORKER}/status", timeout=5)
    return resp2.status_code < 500 and resp3.status_code < 500


def flow_health_check():
    """Simple health check"""
    resp = requests.get(f"{API_GATEWAY}/health", timeout=5)
    return resp.status_code < 500


# ============================================================
# Scenario Definitions
# ============================================================
SCENARIOS = {
    "normal": {
        "label": "Normal Traffic",
        "description": "Balanced user activity — browsing, ordering, viewing history",
        "default_rate": 2,
        "flows": [
            (flow_browse_then_buy, 30),
            (flow_create_order, 25),
            (flow_browse_products, 25),
            (flow_view_orders, 15),
            (flow_health_check, 5),
        ],
    },
    "flash_sale": {
        "label": "Flash Sale",
        "description": "High-volume ordering — simulates promotion/sale event",
        "default_rate": 10,
        "flows": [
            (flow_create_order, 50),
            (flow_browse_then_buy, 30),
            (flow_browse_products, 15),
            (flow_view_orders, 5),
        ],
    },
    "browse_heavy": {
        "label": "Browse Heavy",
        "description": "Mostly browsing with occasional orders — window shoppers",
        "default_rate": 5,
        "flows": [
            (flow_browse_products, 50),
            (flow_view_orders, 20),
            (flow_browse_then_buy, 20),
            (flow_create_order, 10),
        ],
    },
    "health_check": {
        "label": "Health Check Only",
        "description": "Continuous health checks — monitors service availability",
        "default_rate": 3,
        "flows": [
            (flow_health_check, 100),
        ],
    },
    "event_driven": {
        "label": "Event-Driven Pipeline",
        "description": "Orders + verify Kafka event processing (notifications + inventory)",
        "default_rate": 3,
        "flows": [
            (flow_order_then_check_events, 35),
            (flow_create_order, 20),
            (flow_check_notifications, 20),
            (flow_check_inventory, 15),
            (flow_browse_products, 10),
        ],
    },
}


def build_flow_pool(scenario_name):
    """Build weighted list of flow functions for random selection."""
    scenario = SCENARIOS.get(scenario_name, SCENARIOS["normal"])
    pool = []
    for func, weight in scenario["flows"]:
        pool.extend([func] * weight)
    return pool


# ============================================================
# Background Worker
# ============================================================
def traffic_worker(scenario, rate, duration):
    """Run traffic in background thread."""
    pool = build_flow_pool(scenario)
    delay = 1.0 / max(rate, 0.1)
    start_time = time.time()

    print(f"🚀 Traffic started: scenario={scenario}, rate={rate}/s, duration={duration}s")

    while not stop_event.is_set():
        elapsed = time.time() - start_time
        if elapsed >= duration:
            print(f"⏱️ Duration reached ({duration}s). Stopping.")
            break

        try:
            flow = random.choice(pool)
            success = flow()
            with lock:
                state["stats"]["total"] += 1
                if success:
                    state["stats"]["success"] += 1
                else:
                    state["stats"]["errors"] += 1
                state["stats"]["elapsed"] = round(elapsed, 1)
        except Exception as e:
            with lock:
                state["stats"]["total"] += 1
                state["stats"]["errors"] += 1
                state["stats"]["elapsed"] = round(elapsed, 1)
            print(f"  ❌ ERROR: {type(e).__name__}: {e}")

        # Add jitter: ±30% of base delay
        jitter = delay * random.uniform(0.7, 1.3)
        stop_event.wait(jitter)

    with lock:
        state["running"] = False
        state["stats"]["elapsed"] = round(time.time() - start_time, 1)

    print(f"🛑 Traffic stopped. Stats: {state['stats']}")


# ============================================================
# API Endpoints
# ============================================================
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "traffic-gen"})


@app.route("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    with lock:
        running = 1 if state["running"] else 0
        total = state["stats"]["total"]
        success = state["stats"]["success"]
        errors = state["stats"]["errors"]

    lines = [
        "# HELP traffic_gen_running Whether the traffic generator is currently running (1=running, 0=idle)",
        "# TYPE traffic_gen_running gauge",
        f"traffic_gen_running {running}",
        "# HELP traffic_gen_requests_total Total requests sent by traffic generator",
        "# TYPE traffic_gen_requests_total counter",
        f'traffic_gen_requests_total{{result="success"}} {success}',
        f'traffic_gen_requests_total{{result="error"}} {errors}',
        "",
    ]
    from flask import Response
    return Response("\n".join(lines), mimetype="text/plain; version=0.0.4")


@app.route("/status")
def status():
    with lock:
        return jsonify({
            "running": state["running"],
            "scenario": state["scenario"],
            "rate": state["rate"],
            "duration": state["duration"],
            "started_at": state["started_at"],
            "stats": dict(state["stats"]),
            "scenarios": {
                name: {
                    "label": s["label"],
                    "description": s["description"],
                    "default_rate": s["default_rate"],
                }
                for name, s in SCENARIOS.items()
            },
        })


@app.route("/start", methods=["POST"])
def start():
    with lock:
        if state["running"]:
            return jsonify({"error": "Traffic is already running. Stop it first."}), 409

    data = flask_request.get_json(silent=True) or {}
    scenario = data.get("scenario", "normal")
    rate = min(max(int(data.get("rate", SCENARIOS.get(scenario, {}).get("default_rate", 2))), 1), 20)
    duration = min(max(int(data.get("duration", 60)), 10), 300)

    if scenario not in SCENARIOS:
        return jsonify({"error": f"Unknown scenario: {scenario}", "available": list(SCENARIOS.keys())}), 400

    # Reset state
    stop_event.clear()
    with lock:
        state["running"] = True
        state["scenario"] = scenario
        state["rate"] = rate
        state["duration"] = duration
        state["started_at"] = time.time()
        state["stats"] = {"total": 0, "success": 0, "errors": 0, "elapsed": 0}

    # Start worker thread
    t = threading.Thread(target=traffic_worker, args=(scenario, rate, duration), daemon=True)
    t.start()

    return jsonify({
        "message": f"Traffic started: {SCENARIOS[scenario]['label']}",
        "scenario": scenario,
        "rate": rate,
        "duration": duration,
    })


@app.route("/stop", methods=["POST"])
def stop():
    with lock:
        if not state["running"]:
            return jsonify({"message": "Traffic is not running."}), 200

    stop_event.set()

    # Wait briefly for thread to acknowledge stop
    time.sleep(0.5)

    with lock:
        state["running"] = False

    return jsonify({
        "message": "Traffic stopped.",
        "stats": dict(state["stats"]),
    })


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("🚦 Traffic Generator API ready on port 5003")
    print("   Default state: IDLE — use POST /start to begin")
    app.run(host="0.0.0.0", port=5003)
