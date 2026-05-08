"""
============================================================
Sample App - API Gateway (Service A) — Phase 5
============================================================
API Gateway nhận HTTP requests từ user.
Mỗi request tạo 1 trace, gọi tới Order Service (Service B).

Phase 5 additions:
  - Custom metrics: request counter, latency histogram
  - Structured JSON logging with trace_id correlation
  - Enhanced span attributes
============================================================
"""

import os
import time
import random
import logging
import requests
from flask import Flask, jsonify, request as flask_request

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
logger = logging.getLogger("api-gateway")

# ============================================================
# OTEL Setup
# ============================================================
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

resource = Resource.create({
    "service.name": "api-gateway",
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
    export_interval_millis=10000,       # Export mỗi 10s
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# --- Logging Instrumentation (inject trace_id vào log records) ---
LoggingInstrumentor().instrument(set_logging_format=True)

# ============================================================
# Custom Metrics Definition
# ============================================================
# Counter: đếm tổng requests
request_counter = meter.create_counter(
    name="api_gateway_requests_total",
    description="Total HTTP requests received by API Gateway",
    unit="1",
)

# Histogram: đo latency distribution
request_duration = meter.create_histogram(
    name="api_gateway_request_duration_seconds",
    description="HTTP request duration in seconds",
    unit="s",
)

# ============================================================
# Flask App
# ============================================================
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

ORDER_SERVICE = os.getenv("ORDER_SERVICE_URL", "http://order-service:5001")


@app.route("/")
def home():
    return jsonify({"service": "api-gateway", "status": "ok"})


@app.route("/order", methods=["GET", "POST"])
def create_order():
    """Tạo đơn hàng → gọi Order Service"""
    start_time = time.time()
    status = "success"

    with tracer.start_as_current_span("process_order_request") as span:
        # Enhanced span attributes
        items_count = random.randint(1, 10)
        span.set_attribute("order.items_count", items_count)
        span.set_attribute("http.user_agent",
                           flask_request.headers.get("User-Agent", "unknown"))

        # Simulate input validation
        with tracer.start_as_current_span("validate_input") as val_span:
            time.sleep(random.uniform(0.01, 0.05))
            val_span.set_attribute("validation.items_count", items_count)
            val_span.set_attribute("validation.passed", True)

        logger.info("Processing order request",
                     extra={"items_count": items_count})

        # Gọi Order Service
        try:
            resp = requests.get(f"{ORDER_SERVICE}/process")
            data = resp.json()
            order_id = data.get("order_id", "unknown")
            span.set_attribute("order.id", order_id)

            logger.info("Order created successfully",
                         extra={"order_id": order_id, "items_count": items_count})

            result = jsonify({"status": "success", "order": data})

        except Exception as e:
            status = "error"
            span.set_attribute("error", True)
            span.set_attribute("error.message", str(e))

            logger.error("Order creation failed",
                          extra={"error": str(e), "items_count": items_count})

            result = jsonify({"status": "error", "message": str(e)}), 500

    # Record metrics
    duration = time.time() - start_time
    request_counter.add(1, {"endpoint": "/order", "status": status})
    request_duration.record(duration, {"endpoint": "/order", "status": status})

    return result


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    logger.info("API Gateway starting", extra={"port": 5000})
    app.run(host="0.0.0.0", port=5000)
