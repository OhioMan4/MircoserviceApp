"""
============================================================
Notification Worker — Kafka Consumer
============================================================
Consume events từ topic 'order.events' và gửi notifications.

Handles:
  - order.created → "Order confirmation" notification
  - order.payment_completed → "Payment received" notification
  - order.payment_failed → "Payment failed" notification

Features:
  - Idempotency via processed_events table
  - OTel trace context propagation from Kafka headers
  - Custom metrics: notifications_sent_total, processing_duration
  - Structured JSON logging
  - HTTP health endpoint on :5004
============================================================
"""

import os
import time
import json
import uuid
import signal
import logging
import threading
import atexit

import psycopg2
import psycopg2.pool
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException
from flask import Flask, jsonify

# ----------------------------------------------------------
# Connection resilience
# ----------------------------------------------------------
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds, doubles each retry


def retry_connect(name, connect_fn, max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Retry a connection function with exponential backoff."""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = connect_fn()
            logging.getLogger("notification-worker").info(
                f"{name} connected", extra={"attempt": attempt})
            return result
        except Exception as e:
            last_error = e
            wait = delay * (2 ** (attempt - 1))
            logging.getLogger("notification-worker").warning(
                f"{name} connection failed, retrying",
                extra={"attempt": attempt, "max_retries": max_retries,
                       "wait_seconds": wait, "error": str(e)})
            time.sleep(wait)
    raise last_error

# ----------------------------------------------------------
# OpenTelemetry imports
# ----------------------------------------------------------
from opentelemetry import trace, metrics, context as otel_context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.propagate import extract
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator

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
logger = logging.getLogger("notification-worker")

# ============================================================
# OTEL Setup
# ============================================================
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

resource = Resource.create({
    "service.name": "notification-worker",
    "service.version": "1.0.0",
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

# --- Auto-instrumentation ---
LoggingInstrumentor().instrument(set_logging_format=True)
Psycopg2Instrumentor().instrument()

# ============================================================
# Custom Metrics
# ============================================================
notifications_counter = meter.create_counter(
    name="notifications_sent_total",
    description="Total notifications sent",
    unit="1",
)

processing_duration = meter.create_histogram(
    name="notification_processing_duration_seconds",
    description="Notification processing duration",
    unit="s",
)

events_consumed_counter = meter.create_counter(
    name="kafka_events_consumed_total",
    description="Total Kafka events consumed",
    unit="1",
)

# ============================================================
# Config
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order.events")
KAFKA_GROUP = "notification-workers"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app_secret@postgres:5432/orders")

# ============================================================
# Database
# ============================================================
def parse_db_url(url):
    url = url.replace("postgresql://", "")
    userpass, hostdb = url.split("@")
    user, password = userpass.split(":")
    hostport, dbname = hostdb.split("/")
    host, port = hostport.split(":")
    return {"user": user, "password": password,
            "host": host, "port": int(port), "dbname": dbname}

db_params = parse_db_url(DATABASE_URL)
db_pool = None


def get_db_pool():
    global db_pool
    if db_pool is None:
        def _connect():
            return psycopg2.pool.ThreadedConnectionPool(minconn=2, maxconn=5, **db_params)
        db_pool = retry_connect("PostgreSQL", _connect)
    return db_pool


def db_execute(query, params=None, fetch=False):
    pool = get_db_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
            else:
                conn.commit()
                result = cur.rowcount
        return result
    except Exception as e:
        conn.rollback()
        logger.error("DB error", extra={"error": str(e), "query": query[:100]})
        raise
    finally:
        pool.putconn(conn)


def is_event_processed(event_id):
    """Check if event was already processed (idempotency)"""
    rows = db_execute(
        "SELECT 1 FROM processed_events WHERE event_id = %s AND processed_by = %s",
        (event_id, "notification-worker"), fetch=True
    )
    return len(rows) > 0


def mark_event_processed(event_id, event_type):
    """Mark event as processed"""
    db_execute(
        "INSERT INTO processed_events (event_id, event_type, processed_by) VALUES (%s, %s, %s)",
        (event_id, event_type, "notification-worker")
    )


# ============================================================
# Notification Logic
# ============================================================
NOTIFICATION_TEMPLATES = {
    "order.created": {
        "type": "order_confirmation",
        "channel": "email",
        "template": "Your order {order_id} has been received! Product: {product_name}, Qty: {quantity}",
    },
    "order.payment_completed": {
        "type": "payment_success",
        "channel": "email",
        "template": "Payment confirmed for order {order_id}. Amount: ${total_amount}. Txn: {transaction_id}",
    },
    "order.payment_failed": {
        "type": "payment_failure",
        "channel": "email",
        "template": "Payment failed for order {order_id}. Please retry or contact support.",
    },
}


def send_notification(event):
    """Simulate sending notification — log + persist to DB"""
    event_type = event["event_type"]
    order_id = event["order_id"]
    event_id = event["event_id"]
    data = event.get("data", {})

    template_info = NOTIFICATION_TEMPLATES.get(event_type)
    if not template_info:
        logger.warning("No template for event type", extra={"event_type": event_type})
        return

    # Simulate notification delivery delay
    delay = 0.05 + (0.15 * (hash(order_id) % 10) / 10)
    time.sleep(delay)

    # Build message from template
    message = template_info["template"].format(
        order_id=order_id,
        product_name=data.get("product_name", "Unknown"),
        quantity=data.get("quantity", 0),
        total_amount=data.get("total_amount", 0),
        transaction_id=data.get("transaction_id", "N/A"),
    )

    logger.info("Notification sent",
                extra={"order_id": order_id, "type": template_info["type"],
                       "channel": template_info["channel"], "message": message[:100]})

    # Persist to notifications table
    db_execute(
        "INSERT INTO notifications (event_id, order_id, notification_type, channel, status) "
        "VALUES (%s, %s, %s, %s, %s)",
        (event_id, order_id, template_info["type"], template_info["channel"], "sent")
    )

    return template_info["type"]


# ============================================================
# Kafka Consumer Loop
# ============================================================
consumer_running = True
consumer_stats = {"consumed": 0, "processed": 0, "skipped": 0, "errors": 0}
_start_time = time.time()


def extract_trace_context(headers):
    """Extract OTel trace context from Kafka message headers"""
    if not headers:
        return None
    carrier = {}
    for key, value in headers:
        if isinstance(value, bytes):
            carrier[key] = value.decode("utf-8")
        else:
            carrier[key] = str(value)
    return extract(carrier)


def consume_loop():
    """Main Kafka consumer loop"""
    global consumer_running

    logger.info("Starting Kafka consumer",
                extra={"bootstrap": KAFKA_BOOTSTRAP, "topic": KAFKA_TOPIC,
                       "group": KAFKA_GROUP})

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while consumer_running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error", extra={"error": str(msg.error())})
                continue

            start_time = time.time()

            try:
                event = json.loads(msg.value().decode("utf-8"))
                event_id = event.get("event_id", "unknown")
                event_type = event.get("event_type", "unknown")
                order_id = event.get("order_id", "unknown")

                events_consumed_counter.add(1, {"event_type": event_type})
                consumer_stats["consumed"] += 1

                # Extract trace context from Kafka headers
                ctx = extract_trace_context(msg.headers())
                token = None
                if ctx:
                    token = otel_context.attach(ctx)

                try:
                    with tracer.start_as_current_span("kafka.consume") as span:
                        span.set_attribute("messaging.system", "kafka")
                        span.set_attribute("messaging.source", KAFKA_TOPIC)
                        span.set_attribute("messaging.operation", "process")
                        span.set_attribute("event.type", event_type)
                        span.set_attribute("order.id", order_id)
                        span.set_attribute("messaging.kafka.partition", msg.partition())
                        span.set_attribute("messaging.kafka.offset", msg.offset())

                        # Idempotency check
                        if is_event_processed(event_id):
                            span.set_attribute("event.duplicate", True)
                            logger.info("Duplicate event skipped",
                                        extra={"event_id": event_id, "event_type": event_type})
                            consumer_stats["skipped"] += 1
                            continue

                        # Process notification
                        with tracer.start_as_current_span("send_notification") as notif_span:
                            notif_type = send_notification(event)
                            if notif_type:
                                notif_span.set_attribute("notification.type", notif_type)
                                notifications_counter.add(1, {
                                    "type": notif_type,
                                    "event_type": event_type,
                                })

                        # Mark as processed
                        mark_event_processed(event_id, event_type)
                        consumer_stats["processed"] += 1

                        duration = time.time() - start_time
                        processing_duration.record(duration, {"event_type": event_type})
                        span.set_attribute("processing.duration_ms", int(duration * 1000))

                finally:
                    if token:
                        otel_context.detach(token)

            except Exception as e:
                consumer_stats["errors"] += 1
                logger.error("Failed to process Kafka message",
                             extra={"error": str(e), "partition": msg.partition(),
                                    "offset": msg.offset()})

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info("Kafka consumer stopped", extra={"stats": consumer_stats})


# ============================================================
# Flask App (Health + Status)
# ============================================================
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "service": "notification-worker"})


@app.route("/status")
def status():
    uptime = time.time() - _start_time
    return jsonify({
        "service": "notification-worker",
        "status": "running" if consumer_running else "stopped",
        "consumer_group": KAFKA_GROUP,
        "topic": KAFKA_TOPIC,
        "events_processed": consumer_stats["processed"],
        "errors": consumer_stats["errors"],
        "uptime_seconds": round(uptime, 1),
        "stats": consumer_stats,
        "running": consumer_running,
    })


@app.route("/notifications")
def list_notifications():
    """List recent notifications"""
    limit = 30
    try:
        rows = db_execute(
            "SELECT n.event_id, n.order_id, n.notification_type, n.channel, n.status, "
            "n.created_at, pe.event_type "
            "FROM notifications n "
            "LEFT JOIN processed_events pe ON n.event_id = pe.event_id "
            "AND pe.processed_by = 'notification-worker' "
            "ORDER BY n.created_at DESC LIMIT %s",
            (limit,), fetch=True
        )
        notifications = []
        for row in rows:
            n = dict(row)
            n["created_at"] = n["created_at"].isoformat()
            notifications.append(n)
        return jsonify({"notifications": notifications, "count": len(notifications)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# Graceful Shutdown
# ============================================================
def _shutdown_handler(signum, frame):
    """Handle SIGTERM/SIGINT for graceful Kafka consumer shutdown."""
    global consumer_running
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name}, shutting down gracefully...",
                extra={"signal": sig_name})
    consumer_running = False


signal.signal(signal.SIGTERM, _shutdown_handler)
signal.signal(signal.SIGINT, _shutdown_handler)
atexit.register(lambda: logger.info("Notification Worker exiting",
                                     extra={"stats": consumer_stats}))


# ============================================================
# Start Kafka consumer thread (works with both gunicorn and __main__)
# ============================================================
_consumer_thread = threading.Thread(target=consume_loop, daemon=True, name="kafka-consumer")
_consumer_thread.start()
logger.info("Kafka consumer thread started", extra={"threadd": _consumer_thread.name})


# ============================================================
# Main (dev mode only — production uses gunicorn)
# ============================================================
if __name__ == "__main__":
    logger.info("Notification Worker starting (dev mode)",
                extra={"port": 5004, "kafka": KAFKA_BOOTSTRAP, "topic": KAFKA_TOPIC})
    logger.warning("Use gunicorn for production: gunicorn -w 1 -b 0.0.0.0:5004 app:app")
    app.run(host="0.0.0.0", port=5004)
