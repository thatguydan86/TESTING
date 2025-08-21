#!/bin/sh
set -e

echo "🔧 Ensuring Playwright browsers are installed..."
# Download Chromium + Firefox into /root/.cache/ms-playwright (default path)
# If already present, this is a fast no-op.
python -m playwright install chromium firefox || true
echo "✅ Browsers ready."

exec python -u main.py
