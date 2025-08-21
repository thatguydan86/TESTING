FROM python:3.10-slim

WORKDIR /app

# System deps for Playwright browsers (Chromium/Firefox)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget ca-certificates fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libfreetype6 \
    libgcc1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 \
    libpangocairo-1.0-0 libstdc++6 libwayland-client0 libx11-6 libx11-xcb1 libxcb1 \
    libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 \
    libxrender1 libxshmfence1 libdrm2 libgbm1 libxkbcommon0 fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Entrypoint does the 'playwright install' at startup so browsers are guaranteed present
RUN chmod +x /app/start.sh

ENV PYTHONUNBUFFERED=1 TZ=Europe/London
CMD ["/app/start.sh"]
