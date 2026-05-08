FROM python:3.12-slim

# === Metadata ===
LABEL maintainer="observability-lab"
LABEL service="traffic-gen"

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
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5003/health')" || exit 1

# === Run as non-root ===
USER appuser

EXPOSE 5003
CMD ["python", "app.py"]
