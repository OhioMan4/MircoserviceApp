"""
============================================================
Unit Tests — API Gateway
============================================================
Test các endpoint HTTP mà KHÔNG cần chạy external services.
Sử dụng Flask test client + unittest.mock để mock dependencies.

Chạy: pytest test_app.py -v
============================================================
"""

import pytest
from unittest.mock import patch, MagicMock


# ============================================================
# Fixture: Flask test client
# ============================================================
# Phải mock OTel trước khi import app vì app.py khởi tạo
# OTel exporters ngay khi import (side effect ở module level).
# ============================================================

@pytest.fixture
def client():
    """Tạo Flask test client — mock OTel để không cần collector"""
    # Mock OTel exporters để tránh connection errors khi import
    with patch("opentelemetry.sdk.trace.export.BatchSpanProcessor"), \
         patch("opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter"), \
         patch("opentelemetry.exporter.otlp.proto.grpc.metric_exporter.OTLPMetricExporter"), \
         patch("opentelemetry.sdk.metrics.export.PeriodicExportingMetricReader"):

        # Import app SAU KHI mock — tránh connect tới otel-collector
        import importlib
        import app as app_module
        importlib.reload(app_module)

        app_module.app.config['TESTING'] = True
        with app_module.app.test_client() as test_client:
            yield test_client


# ============================================================
# Test /health endpoint
# ============================================================
class TestHealthEndpoint:
    """Health check phải luôn trả về 200 + status healthy"""

    def test_health_returns_200(self, client):
        resp = client.get('/health')
        assert resp.status_code == 200

    def test_health_returns_healthy_status(self, client):
        resp = client.get('/health')
        data = resp.get_json()
        assert data['status'] == 'healthy'


# ============================================================
# Test / (home) endpoint
# ============================================================
class TestHomeEndpoint:
    """Home endpoint trả về thông tin service"""

    def test_home_returns_service_name(self, client):
        resp = client.get('/')
        data = resp.get_json()
        assert data['service'] == 'api-gateway'
        assert data['status'] == 'ok'

    def test_home_lists_endpoints(self, client):
        resp = client.get('/')
        data = resp.get_json()
        assert '/order' in data['endpoints']
        assert '/health' in data['endpoints']


# ============================================================
# Test /order endpoint
# ============================================================
class TestOrderEndpoint:
    """Test tạo order — mock downstream order-service"""

    @patch('app.requests.post')
    def test_create_order_success(self, mock_post, client):
        """Khi order-service trả về success"""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                'order_id': 'ORD-001',
                'status': 'completed',
                'product': 'Test Product',
                'quantity': 2,
                'total_amount': 100.0,
            }
        )

        resp = client.post('/order',
                           json={'product_id': 1, 'quantity': 2},
                           content_type='application/json')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'

    @patch('app.requests.post')
    def test_create_order_downstream_error(self, mock_post, client):
        """Khi order-service không available"""
        mock_post.side_effect = ConnectionError("Connection refused")

        resp = client.post('/order',
                           json={'product_id': 1, 'quantity': 1},
                           content_type='application/json')
        assert resp.status_code == 500
        data = resp.get_json()
        assert data['status'] == 'error'

    @patch('app.requests.post')
    def test_create_order_without_body(self, mock_post, client):
        """Khi gọi GET /order (random product_id)"""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {'order_id': 'ORD-002', 'status': 'completed'}
        )

        resp = client.get('/order')
        assert resp.status_code == 200


# ============================================================
# Test /products endpoint
# ============================================================
class TestProductsEndpoint:
    """Test proxy products — mock downstream call"""

    @patch('app.requests.get')
    def test_list_products_success(self, mock_get, client):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                'products': [
                    {'id': 1, 'name': 'Laptop', 'price': 999.99},
                    {'id': 2, 'name': 'Phone', 'price': 699.99},
                ],
                'source': 'database'
            }
        )

        resp = client.get('/products')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['products']) == 2

    @patch('app.requests.get')
    def test_list_products_downstream_error(self, mock_get, client):
        """Khi order-service fail → trả về error"""
        mock_get.side_effect = ConnectionError("Connection refused")

        resp = client.get('/products')
        assert resp.status_code == 200  # Flask catches and returns error JSON
        data = resp.get_json()
        assert 'error' in data
