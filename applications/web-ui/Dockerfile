FROM nginx:alpine

# === Metadata ===
LABEL maintainer="observability-lab"
LABEL service="web-ui"

# === Application files ===
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
COPY style.css  /usr/share/nginx/html/style.css
COPY app.js     /usr/share/nginx/html/app.js

# === Health check ===
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget -q --spider http://localhost:8080/ || exit 1

EXPOSE 8080
