# Has Firefox/Chromium/WebKit + all system deps preinstalled
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 TZ=Europe/London
CMD ["python", "-u", "main.py"]
