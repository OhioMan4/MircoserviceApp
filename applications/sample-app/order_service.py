"""
============================================================
Sample App - Order Service (Service B) — Phase 5
============================================================
Xử lý đơn hàng. Gọi Payment Service (Service C) để thanh toán.

Phase 5 additions:
  - Custom metrics: orders counter, processing duration histogram
  - Structured JSON logging with trace_id correlation
  - Enhanced span attributes for each processing step
============================================================
"""

import os
import time
import random
import uuid
import logging
import requests
from flask import Flask, jsonify

# ----------------------------------------------------------
# OpenTelemetry imports
# ----------------------------------------------------------
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# ----------------------------------------------------------
# Structured JSON logging
# ----------------------------------------------------------
from pythonjsonlogger import json as json_logger

handler = logging.StreamHandler()
handler.setFormatter(json_logger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    rename_fields={"asctime": "timestamp", "levelname": "level"},
))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("order-service")

# ============================================================
# OTEL Setup
# ============================================================
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

resource = Resource.create({
    "service.name": "order-service",
    "service.version": "2.0.0",
})

# --- Tracing ---
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
)
trace.set_tracer_provider(trace_provider)
set_global_textmap(CompositePropagator([TraceContextTextMapPropagator()]))
tracer = trace.get_tracer(__name__)

# --- Metrics ---
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True),
    export_interval_millis=10000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# --- Logging Instrumentation ---
LoggingInstrumentor().instrument(set_logging_format=True)

# ============================================================
# Custom Metrics
# ============================================================
orders_counter = meter.create_counter(
    name="orders_created_total",
    description="Total orders created",
    unit="1",
)

order_duration = meter.create_histogram(
    name="order_processing_duration_seconds",
    description="Order processing duration in seconds",
    unit="s",
)

inventory_check_counter = meter.create_counter(
    name="inventory_checks_total",
    description="Total inventory checks performed",
    unit="1",
)

# ============================================================
# Flask App
# ============================================================
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

PAYMENT_SERVICE = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:5002")


@app.route("/process")
def process_order():
    """Xử lý đơn hàng"""
    start_time = time.time()
    order_id = str(uuid.uuid4())[:8]
    order_status = "completed"

    logger.info("Processing new order", extra={"order_id": order_id})

    # Step 1: Validate order
    with tracer.start_as_current_span("validate_order") as span:
        time.sleep(random.uniform(0.02, 0.08))
        span.set_attribute("order.id", order_id)
        span.set_attribute("validation.passed", True)
        logger.info("Order validated", extra={"order_id": order_id})

    # Step 2: Check inventory
    with tracer.start_as_current_span("check_inventory") as span:
        time.sleep(random.uniform(0.01, 0.05))
        in_stock = random.random() > 0.1    # 90% in stock
        span.set_attribute("inventory.in_stock", in_stock)
        span.set_attribute("inventory.check_method", "database")

        inventory_check_counter.add(1, {"result": "in_stock" if in_stock else "out_of_stock"})
        logger.info("Inventory checked",
                     extra={"order_id": order_id, "in_stock": in_stock})

        if not in_stock:
            order_status = "out_of_stock"
            span.set_attribute("order.status", order_status)

    # Step 3: Process payment
    with tracer.start_as_current_span("request_payment") as span:
        try:
            resp = requests.get(f"{PAYMENT_SERVICE}/charge?order_id={order_id}")
            payment = resp.json()
            payment_status = payment.get("status", "unknown")
            span.set_attribute("payment.status", payment_status)

            if resp.status_code != 200:
                order_status = "payment_failed"

            logger.info("Payment processed",
                         extra={"order_id": order_id, "payment_status": payment_status})

        except Exception as e:
            span.set_attribute("error", True)
            order_status = "payment_error"
            payment = {"status": "failed", "error": str(e)}
            logger.error("Payment request failed",
                          extra={"order_id": order_id, "error": str(e)})

    # Record metrics
    duration = time.time() - start_time
    orders_counter.add(1, {"status": order_status})
    order_duration.record(duration, {"status": order_status})

    logger.info("Order completed",
                 extra={"order_id": order_id, "status": order_status,
                        "duration_ms": int(duration * 1000)})

    return jsonify({
        "order_id": order_id,
        "payment": payment,
        "status": order_status
    })


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    logger.info("Order Service starting", extra={"port": 5001})
    app.run(host="0.0.0.0", port=5001)
