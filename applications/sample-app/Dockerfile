FROM python:3.12-slim

# === Metadata ===
LABEL maintainer="observability-lab"
LABEL service="sample-app"

# === Security: non-root user ===
RUN groupadd -r appuser && useradd -r -g appuser appuser

# === Dependencies (layer cache) ===
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# === Application code ===
COPY . .

# === Health check (api_gateway mặc định chạy port 5000) ===
HEALTHCHECK --interval=10s --timeout=3s --retries=3 --start-period=5s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')" || exit 1

# === Run as non-root ===
USER appuser

EXPOSE 5000
# Entrypoint sẽ được override bởi docker-compose command
CMD ["python", "api_gateway.py"]
