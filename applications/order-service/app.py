"""
============================================================
Order Service — Phase 5 + DB/Cache Expansion
============================================================
Xử lý đơn hàng với PostgreSQL persistence và Redis cache.

Features:
  - PostgreSQL: persist orders, query products (with connection pool)
  - Redis: cache-aside pattern cho product catalog (TTL 60s)
  - OTel auto-instrumentation: psycopg2 + redis → spans tự động
  - Custom metrics: connection pool, cache hit/miss, query duration
  - Endpoints: /process, /products, /orders
============================================================
"""

import os
import time
import json
import random
import uuid
import logging
import requests
from confluent_kafka import Producer as KafkaProducer

import psycopg2
import psycopg2.pool
import psycopg2.extras
import redis as redis_lib
from flask import Flask, jsonify, request as flask_request

# ----------------------------------------------------------
# Connection resilience
# ----------------------------------------------------------
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds, doubles each retry


def retry_connect(name, connect_fn, max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Retry a connection function with exponential backoff.

    Args:
        name: Human-readable name for logging
        connect_fn: Callable that returns the connection/client
        max_retries: Maximum number of retry attempts
        delay: Initial delay in seconds (doubles each retry)

    Returns:
        The connection object from connect_fn

    Raises:
        Last exception if all retries exhausted
    """
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = connect_fn()
            logger.info(f"{name} connected", extra={"attempt": attempt})
            return result
        except Exception as e:
            last_error = e
            wait = delay * (2 ** (attempt - 1))
            logger.warning(f"{name} connection failed, retrying",
                           extra={"attempt": attempt, "max_retries": max_retries,
                                  "wait_seconds": wait, "error": str(e)})
            time.sleep(wait)
    logger.error(f"{name} connection failed after {max_retries} retries",
                 extra={"error": str(last_error)})
    raise last_error

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
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
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
    "service.version": "3.0.0",
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

# --- Auto-instrumentation (BEFORE creating connections) ---
LoggingInstrumentor().instrument(set_logging_format=True)
Psycopg2Instrumentor().instrument()
RedisInstrumentor().instrument()

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

# Database metrics
db_query_duration = meter.create_histogram(
    name="db_query_duration_seconds",
    description="Database query duration in seconds",
    unit="s",
)

db_pool_active = meter.create_up_down_counter(
    name="db_connection_pool_active",
    description="Active database connections in pool",
    unit="1",
)

# Cache metrics
cache_ops_counter = meter.create_counter(
    name="cache_operations_total",
    description="Cache operations (hit/miss/set/error)",
    unit="1",
)

cache_duration = meter.create_histogram(
    name="cache_operation_duration_seconds",
    description="Cache operation latency",
    unit="s",
)

# Kafka metrics
kafka_produced_counter = meter.create_counter(
    name="kafka_messages_produced_total",
    description="Total Kafka messages produced",
    unit="1",
)

# ============================================================
# Kafka Producer Setup
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "order.events"

kafka_producer = None


def get_kafka_producer():
    """Lazy init Kafka producer with retry"""
    global kafka_producer
    if kafka_producer is None:
        def _connect():
            producer = KafkaProducer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "client.id": "order-service",
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 100,
            })
            # Verify connectivity by listing topics
            producer.list_topics(timeout=5)
            return producer
        logger.info("Initializing Kafka producer", extra={"bootstrap": KAFKA_BOOTSTRAP})
        kafka_producer = retry_connect("Kafka", _connect)
    return kafka_producer


def kafka_delivery_callback(err, msg):
    """Callback for Kafka produce delivery report"""
    if err:
        logger.error("Kafka delivery failed",
                     extra={"topic": msg.topic(), "error": str(err)})
        kafka_produced_counter.add(1, {"status": "failed", "topic": msg.topic()})
    else:
        logger.info("Kafka message delivered",
                    extra={"topic": msg.topic(), "partition": msg.partition(),
                           "offset": msg.offset()})
        kafka_produced_counter.add(1, {"status": "success", "topic": msg.topic()})


def publish_event(event_type, order_id, data):
    """Publish event to Kafka with trace context propagation"""
    event = {
        "event_type": event_type,
        "event_id": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "order_id": order_id,
        "data": data,
    }

    # Inject trace context into Kafka headers
    headers = {}
    from opentelemetry.propagate import inject
    inject(headers)
    kafka_headers = [(k, v.encode("utf-8") if isinstance(v, str) else v)
                     for k, v in headers.items()]

    with tracer.start_as_current_span("kafka.produce") as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination", KAFKA_TOPIC)
        span.set_attribute("messaging.operation", "publish")
        span.set_attribute("event.type", event_type)
        span.set_attribute("order.id", order_id)

        try:
            producer = get_kafka_producer()
            producer.produce(
                topic=KAFKA_TOPIC,
                key=order_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                headers=kafka_headers,
                callback=kafka_delivery_callback,
            )
            producer.poll(0)  # Trigger delivery callbacks
            logger.info("Event published to Kafka",
                        extra={"event_type": event_type, "order_id": order_id,
                               "topic": KAFKA_TOPIC})
        except Exception as e:
            span.set_attribute("error", True)
            logger.error("Failed to publish Kafka event",
                         extra={"event_type": event_type, "order_id": order_id,
                                "error": str(e)})


# ============================================================
# Database + Cache Setup
# ============================================================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app_secret@postgres:5432/orders")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

CACHE_TTL = 60  # seconds

# Parse DATABASE_URL components
def parse_db_url(url):
    """Parse postgresql://user:pass@host:port/dbname"""
    url = url.replace("postgresql://", "")
    userpass, hostdb = url.split("@")
    user, password = userpass.split(":")
    hostport, dbname = hostdb.split("/")
    host, port = hostport.split(":")
    return {
        "user": user, "password": password,
        "host": host, "port": int(port), "dbname": dbname,
    }

db_params = parse_db_url(DATABASE_URL)

# Connection pool (will be initialized on first request)
db_pool = None
redis_client = None


def get_db_pool():
    """Lazy init DB connection pool with retry"""
    global db_pool
    if db_pool is None:
        def _connect():
            return psycopg2.pool.ThreadedConnectionPool(
                minconn=2, maxconn=10, **db_params
            )
        logger.info("Initializing PostgreSQL connection pool",
                     extra={"host": db_params["host"], "db": db_params["dbname"]})
        db_pool = retry_connect("PostgreSQL", _connect)
    return db_pool


def get_redis():
    """Lazy init Redis client with retry"""
    global redis_client
    if redis_client is None:
        def _connect():
            client = redis_lib.from_url(REDIS_URL, decode_responses=True)
            client.ping()  # Verify connection
            return client
        logger.info("Initializing Redis connection", extra={"url": REDIS_URL})
        redis_client = retry_connect("Redis", _connect)
    return redis_client


def db_query(query, params=None, fetch=True):
    """Execute DB query with metrics tracking"""
    pool = get_db_pool()
    conn = pool.getconn()
    db_pool_active.add(1)
    start = time.time()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
            else:
                conn.commit()
                result = cur.rowcount
        duration = time.time() - start
        # Determine operation type from query
        op = query.strip().split()[0].upper()
        db_query_duration.record(duration, {"operation": op.lower()})

        if duration > 0.1:
            logger.warning("Slow database query",
                           extra={"query": query[:100], "duration_ms": int(duration * 1000),
                                  "operation": op})
        return result
    except Exception as e:
        conn.rollback()
        logger.error("Database query failed",
                     extra={"query": query[:100], "error": str(e)})
        raise
    finally:
        pool.putconn(conn)
        db_pool_active.add(-1)


def cache_get(key):
    """Get from Redis cache with metrics"""
    start = time.time()
    try:
        r = get_redis()
        value = r.get(key)
        duration = time.time() - start
        cache_duration.record(duration, {"operation": "get"})

        if value is not None:
            cache_ops_counter.add(1, {"operation": "get", "result": "hit"})
            logger.info("Cache hit", extra={"key": key})
            return json.loads(value)
        else:
            cache_ops_counter.add(1, {"operation": "get", "result": "miss"})
            logger.info("Cache miss", extra={"key": key})
            return None
    except Exception as e:
        cache_ops_counter.add(1, {"operation": "get", "result": "error"})
        logger.error("Cache get failed", extra={"key": key, "error": str(e)})
        return None


def cache_set(key, value, ttl=CACHE_TTL):
    """Set Redis cache with metrics"""
    start = time.time()
    try:
        r = get_redis()
        r.setex(key, ttl, json.dumps(value))
        duration = time.time() - start
        cache_duration.record(duration, {"operation": "set"})
        cache_ops_counter.add(1, {"operation": "set", "result": "ok"})
    except Exception as e:
        cache_ops_counter.add(1, {"operation": "set", "result": "error"})
        logger.error("Cache set failed", extra={"key": key, "error": str(e)})


# ============================================================
# Flask App
# ============================================================
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

PAYMENT_SERVICE = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:5002")


@app.route("/products")
def list_products():
    """List product catalog (cache-aside pattern)"""
    with tracer.start_as_current_span("get_product_catalog") as span:
        # 1. Try cache first
        products = cache_get("product:catalog")

        if products is not None:
            span.set_attribute("cache.hit", True)
            span.set_attribute("products.count", len(products))
            return jsonify({"products": products, "source": "cache"})

        # 2. Cache miss → query DB
        span.set_attribute("cache.hit", False)
        logger.info("Fetching products from database")

        rows = db_query("SELECT id, name, price, stock, category FROM products ORDER BY id")
        products = [dict(row) for row in rows]

        # Convert Decimal to float for JSON
        for p in products:
            p["price"] = float(p["price"])

        # 3. Set cache
        cache_set("product:catalog", products)

        span.set_attribute("products.count", len(products))
        return jsonify({"products": products, "source": "database"})


@app.route("/orders")
def list_orders():
    """List recent orders"""
    with tracer.start_as_current_span("list_recent_orders") as span:
        limit = flask_request.args.get("limit", 20, type=int)
        rows = db_query(
            "SELECT order_id, product_name, quantity, total_amount, status, "
            "payment_txn_id, created_at FROM orders ORDER BY created_at DESC LIMIT %s",
            (limit,)
        )
        orders = []
        for row in rows:
            o = dict(row)
            o["total_amount"] = float(o["total_amount"])
            o["created_at"] = o["created_at"].isoformat()
            orders.append(o)
        span.set_attribute("orders.count", len(orders))
        return jsonify({"orders": orders, "count": len(orders)})


@app.route("/process", methods=["GET", "POST"])
def process_order():
    """Xử lý đơn hàng — now with real DB persistence"""
    start_time = time.time()
    order_id = str(uuid.uuid4())[:8]
    order_status = "completed"

    # Parse request body
    if flask_request.method == "POST" and flask_request.is_json:
        data = flask_request.get_json()
        product_id = data.get("product_id", random.randint(1, 5))
        quantity = data.get("quantity", random.randint(1, 3))
    else:
        product_id = random.randint(1, 5)
        quantity = random.randint(1, 3)

    logger.info("Processing new order",
                 extra={"order_id": order_id, "product_id": product_id, "quantity": quantity})

    # Step 1: Get product info (from cache or DB)
    with tracer.start_as_current_span("get_product_info") as span:
        span.set_attribute("product.id", product_id)

        # Try cache first
        product = None
        cached_catalog = cache_get("product:catalog")
        if cached_catalog:
            for p in cached_catalog:
                if p["id"] == product_id:
                    product = p
                    break

        if product is None:
            rows = db_query("SELECT id, name, price, stock FROM products WHERE id = %s",
                            (product_id,))
            if rows:
                product = dict(rows[0])
                product["price"] = float(product["price"])
            else:
                return jsonify({"error": f"Product {product_id} not found"}), 404

        span.set_attribute("product.name", product["name"])
        span.set_attribute("product.price", product["price"])

    # Step 2: Check inventory
    with tracer.start_as_current_span("check_inventory") as span:
        rows = db_query("SELECT stock FROM products WHERE id = %s", (product_id,))
        current_stock = rows[0]["stock"] if rows else 0
        in_stock = current_stock >= quantity

        span.set_attribute("inventory.current_stock", current_stock)
        span.set_attribute("inventory.requested", quantity)
        span.set_attribute("inventory.in_stock", in_stock)

        logger.info("Inventory checked",
                     extra={"order_id": order_id, "product_id": product_id,
                            "stock": current_stock, "in_stock": in_stock})

        if not in_stock:
            order_status = "out_of_stock"
            orders_counter.add(1, {"status": order_status})
            return jsonify({
                "order_id": order_id,
                "status": order_status,
                "message": f"Insufficient stock (have {current_stock}, need {quantity})"
            }), 409

    total_amount = round(product["price"] * quantity, 2)

    # Step 3: Insert order into DB
    with tracer.start_as_current_span("insert_order") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("order.total_amount", total_amount)

        db_query(
            "INSERT INTO orders (order_id, product_id, product_name, quantity, total_amount, status) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (order_id, product_id, product["name"], quantity, total_amount, "pending"),
            fetch=False
        )
        logger.info("Order inserted into database",
                     extra={"order_id": order_id, "total_amount": total_amount})

    # Step 4: Update stock
    with tracer.start_as_current_span("update_stock") as span:
        db_query(
            "UPDATE products SET stock = stock - %s WHERE id = %s AND stock >= %s",
            (quantity, product_id, quantity),
            fetch=False
        )
        span.set_attribute("stock.deducted", quantity)

        # Invalidate product cache since stock changed
        try:
            get_redis().delete("product:catalog")
            cache_ops_counter.add(1, {"operation": "delete", "result": "ok"})
            logger.info("Product cache invalidated after stock update")
        except Exception:
            pass

    # Step 5: Process payment
    payment = {"status": "skipped"}
    with tracer.start_as_current_span("request_payment") as span:
        try:
            resp = requests.post(
                f"{PAYMENT_SERVICE}/charge",
                json={"order_id": order_id, "amount": total_amount},
                timeout=30,
            )
            payment = resp.json()
            payment_status = payment.get("status", "unknown")
            span.set_attribute("payment.status", payment_status)

            if resp.status_code != 200:
                order_status = "payment_failed"
            else:
                # Update order with payment info
                txn_id = payment.get("transaction_id", "")
                db_query(
                    "UPDATE orders SET status = %s, payment_txn_id = %s, updated_at = NOW() "
                    "WHERE order_id = %s",
                    (order_status, txn_id, order_id),
                    fetch=False
                )

            logger.info("Payment processed",
                         extra={"order_id": order_id, "payment_status": payment_status})

        except Exception as e:
            span.set_attribute("error", True)
            order_status = "payment_error"
            payment = {"status": "failed", "error": str(e)}
            logger.error("Payment request failed",
                          extra={"order_id": order_id, "error": str(e)})

    # ── Publish Kafka events ──
    event_data = {
        "product_id": product_id,
        "product_name": product["name"],
        "quantity": quantity,
        "total_amount": total_amount,
    }

    # Always publish order.created
    publish_event("order.created", order_id, event_data)

    # Publish payment result event
    if order_status == "completed":
        event_data["payment_status"] = "success"
        event_data["transaction_id"] = payment.get("transaction_id", "")
        publish_event("order.payment_completed", order_id, event_data)
    elif order_status in ("payment_failed", "payment_error"):
        event_data["payment_status"] = "failed"
        event_data["error"] = payment.get("error", "unknown")
        publish_event("order.payment_failed", order_id, event_data)

    # Flush Kafka producer (ensure delivery)
    try:
        get_kafka_producer().flush(timeout=2)
    except Exception:
        pass

    # Record metrics
    duration = time.time() - start_time
    orders_counter.add(1, {"status": order_status})
    order_duration.record(duration, {"status": order_status})

    logger.info("Order completed",
                 extra={"order_id": order_id, "status": order_status,
                        "duration_ms": int(duration * 1000),
                        "product": product["name"], "total_amount": total_amount})

    return jsonify({
        "order_id": order_id,
        "product": product["name"],
        "quantity": quantity,
        "total_amount": total_amount,
        "payment": payment,
        "status": order_status,
    })


@app.route("/health")
def health():
    """Health check with DB + Redis connectivity"""
    health_status = {"status": "healthy", "db": "unknown", "cache": "unknown"}
    try:
        db_query("SELECT 1", fetch=True)
        health_status["db"] = "connected"
    except Exception as e:
        health_status["db"] = f"error: {str(e)}"
        health_status["status"] = "degraded"

    try:
        get_redis().ping()
        health_status["cache"] = "connected"
    except Exception as e:
        health_status["cache"] = f"error: {str(e)}"
        health_status["status"] = "degraded"

    status_code = 200 if health_status["status"] == "healthy" else 503
    return jsonify(health_status), status_code


if __name__ == "__main__":
    logger.info("Order Service starting (dev mode)",
                 extra={"port": 5001, "db_host": db_params["host"],
                        "redis_url": REDIS_URL})
    logger.warning("Use gunicorn for production: gunicorn -w 4 -b 0.0.0.0:5001 app:app")
    app.run(host="0.0.0.0", port=5001)
