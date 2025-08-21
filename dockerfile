FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Force Python to run in unbuffered mode so logs flush immediately
CMD ["python", "-u", "main.py"]
