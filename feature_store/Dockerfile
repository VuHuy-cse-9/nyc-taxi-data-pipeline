FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml and install dependencies
COPY ../pyproject.toml /app/
RUN pip install --no-cache-dir -e /app

# Copy feature_store directory
COPY . /app/feature_store/

WORKDIR /app

# Expose port 8000
EXPOSE 8000

# Run the FastAPI app
CMD ["uvicorn", "feature_store.app:app", "--host", "0.0.0.0", "--port", "8000"]
