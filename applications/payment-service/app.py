"""
============================================================
Payment Service — Phase 5
============================================================
Xử lý thanh toán. Simulate delays và random errors.
Metrics: payments counter, amount histogram, gateway duration
============================================================
"""

import os
import time
import random
import logging
from flask import Flask, jsonify, request

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
logger = logging.getLogger("payment-service")

# ============================================================
# OTEL Setup
# ============================================================
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

resource = Resource.create({
    "service.name": "payment-service",
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
payments_counter = meter.create_counter(
    name="payments_total",
    description="Total payment transactions",
    unit="1",
)

payment_amount = meter.create_histogram(
    name="payment_amount_dollars",
    description="Payment amount distribution in dollars",
    unit="$",
)

gateway_duration = meter.create_histogram(
    name="payment_gateway_duration_seconds",
    description="External payment gateway call duration",
    unit="s",
)

# ============================================================
# Flask App
# ============================================================
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

# Simulated payment providers
PROVIDERS = ["stripe", "paypal", "square"]


@app.route("/charge", methods=["POST"])
def charge():
    """Xử lý thanh toán"""
    # Support both JSON body (preferred) and query params (backward compat)
    if request.is_json:
        data = request.get_json()
        order_id = data.get("order_id", "unknown")
        amount = data.get("amount")
    else:
        order_id = request.args.get("order_id", "unknown")
        amount = request.args.get("amount")

    if amount:
        amount = float(amount)
    else:
        amount = round(random.uniform(10.0, 500.0), 2)

    provider = random.choice(PROVIDERS)

    logger.info("Processing payment",
                 extra={"order_id": order_id, "amount": amount, "provider": provider})

    # Step 1: Validate payment method
    with tracer.start_as_current_span("validate_payment_method") as span:
        time.sleep(random.uniform(0.01, 0.03))
        span.set_attribute("payment.order_id", order_id)
        span.set_attribute("payment.provider", provider)
        span.set_attribute("payment.amount", amount)

    # Step 2: Call external payment gateway
    with tracer.start_as_current_span("call_payment_gateway") as span:
        span.set_attribute("payment.provider", provider)
        span.set_attribute("payment.amount", amount)

        # Simulate gateway latency
        delay = random.uniform(0.05, 0.15)
        is_slow = random.random() < 0.2

        if is_slow:
            delay = random.uniform(0.5, 2.0)
            span.set_attribute("payment.slow", True)
            logger.warning("Slow payment gateway response",
                            extra={"order_id": order_id, "provider": provider,
                                   "delay_ms": int(delay * 1000)})

        time.sleep(delay)

        # Record gateway duration metric
        gateway_duration.record(delay, {"provider": provider})
        span.set_attribute("payment.gateway_duration_ms", int(delay * 1000))

        # Simulate payment failure (10% chance)
        if random.random() < 0.1:
            span.set_attribute("error", True)
            span.set_attribute("error.message", "Payment gateway timeout")

            payments_counter.add(1, {"status": "failed", "provider": provider})
            payment_amount.record(amount, {"status": "failed", "provider": provider})

            logger.error("Payment failed",
                          extra={"order_id": order_id, "provider": provider,
                                 "amount": amount, "error": "gateway_timeout"})

            return jsonify({
                "status": "failed",
                "error": "Payment gateway timeout",
                "order_id": order_id,
                "provider": provider,
            }), 500

    # Success
    txn_id = f"txn-{random.randint(10000, 99999)}"

    payments_counter.add(1, {"status": "success", "provider": provider})
    payment_amount.record(amount, {"status": "success", "provider": provider})

    logger.info("Payment successful",
                 extra={"order_id": order_id, "provider": provider,
                        "amount": amount, "transaction_id": txn_id})

    return jsonify({
        "status": "success",
        "order_id": order_id,
        "transaction_id": txn_id,
        "provider": provider,
        "amount": amount,
    })


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    logger.info("Payment Service starting (dev mode)", extra={"port": 5002})
    logger.warning("Use gunicorn for production: gunicorn -w 4 -b 0.0.0.0:5002 app:app")
    app.run(host="0.0.0.0", port=5002)
