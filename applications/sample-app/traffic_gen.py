"""
============================================================
Traffic Generator - Tự động gửi requests để tạo traces
============================================================
Script này chạy liên tục, gửi requests tới API Gateway
mỗi 2-5 giây để tạo traces cho việc quan sát.
============================================================
"""

import time
import random
import requests

API_GATEWAY = "http://api-gateway:5000"

print("🚀 Traffic Generator started! Sending requests...")

while True:
    try:
        # Random chọn endpoint
        endpoint = random.choice(["/order", "/order", "/order", "/"])
        resp = requests.get(f"{API_GATEWAY}{endpoint}", timeout=10)
        print(f"  → {endpoint} → {resp.status_code} ({resp.elapsed.total_seconds():.3f}s)")
    except Exception as e:
        print(f"  → ERROR: {e}")

    # Random delay 2-5 giây
    time.sleep(random.uniform(2, 5))
