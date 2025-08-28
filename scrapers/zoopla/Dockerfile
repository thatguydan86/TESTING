FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy
WORKDIR /app
# Copy the entire repository into the container (context is root of repo in GH action)
COPY . /app
# Install Python dependencies; ignore failure if requirements don't exist
RUN pip install -r /app/requirements.txt || true
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "scrapers.zoopla.main"]
