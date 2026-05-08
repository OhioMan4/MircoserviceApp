FROM python:3.12-slim

# === Metadata ===
LABEL maintainer="observability-lab"
LABEL service="notification-worker"

# === Security: non-root user ===
RUN groupadd -r appuser && useradd -r -g appuser appuser

# === Dependencies (layer cache) ===
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# === Application code ===
COPY app.py .

# === Health check ===
HEALTHCHECK --interval=10s --timeout=3s --retries=3 --start-period=10s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5004/health')" || exit 1

# === Run as non-root ===
USER appuser

EXPOSE 5004
CMD ["gunicorn", "--workers", "1", "--bind", "0.0.0.0:5004", "--access-logfile", "-", "--timeout", "30", "app:app"]
