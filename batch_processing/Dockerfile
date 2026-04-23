FROM apache/spark:4.0.2-java21

USER root

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system deps including curl
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN uv sync --extra spark

CMD sleep infinity