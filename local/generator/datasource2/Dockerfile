FROM python:3.11-slim

# Environment settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install psycopg with binary + pool extras
RUN pip install --no-cache-dir \
    "python-dotenv" \
    "cassandra-driver==3.29.2" \
    "pandas"

# Copy application code
COPY main.py main.py

CMD ["python", "main.py"]
