# Includes Chromium/Firefox/WebKit + all deps
FROM mcr.microsoft.com/playwright/python:v1.46.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 TZ=Europe/London PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
# (optional) force fresh build when needed
ARG CACHE_BUST=2025-08-21-1705

CMD ["python", "-u", "main.py"]
