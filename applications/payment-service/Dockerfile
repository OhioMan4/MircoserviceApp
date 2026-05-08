FROM python:3.12-slim

# === Metadata ===
LABEL maintainer="observability-lab"
LABEL service="payment-service"

# === Security: non-root user ===
RUN groupadd -r appuser && useradd -r -g appuser appuser

# === Dependencies (layer cache) ===
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# === Application code ===
COPY app.py .

# === Health check ===
HEALTHCHECK --interval=10s --timeout=3s --retries=3 --start-period=5s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5002/health')" || exit 1

# === Run as non-root ===
USER appuser

EXPOSE 5002
CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5002", "--access-logfile", "-", "--timeout", "30", "app:app"]
