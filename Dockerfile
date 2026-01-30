# versostat-pyscraper - Python scraper for FPL and Sportmonks
FROM python:3.12-slim

WORKDIR /app

# Install curl for RDS CA bundle download
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Add the Amazon RDS trust bundle (covers all regions)
RUN mkdir -p /etc/ssl/certs \
    && curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
        -o /etc/ssl/certs/rds-global-bundle.pem \
    && chmod 0644 /etc/ssl/certs/rds-global-bundle.pem

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY index.py .
COPY src/ ./src/

# Run as non-root
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default CMD (overridden by Step Functions)
CMD ["python", "index.py"]
