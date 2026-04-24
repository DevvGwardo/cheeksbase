FROM python:3.11-slim

WORKDIR /app

# Install build dependencies (needed for some compiled Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy build metadata and source
COPY pyproject.toml README.md ./
COPY cheeksbase ./cheeksbase

# Install the package plus uvicorn (runtime import in cheeksbase.mcp.server)
RUN pip install --no-cache-dir . uvicorn

# Create the data directory that will hold DuckDB, config, and cache
RUN mkdir -p /data && chmod 755 /data

# Environment
ENV CHEEKSBASE_DIR=/data
ENV PYTHONUNBUFFERED=1

# Run as non-root
RUN useradd -m -u 1000 cheeksbase && chown -R cheeksbase:cheeksbase /data /app
USER cheeksbase

EXPOSE 8000

# Use shell form so $HOST/$PORT env vars are expanded at runtime
CMD sh -c 'cheeksbase serve --host "${HOST:-0.0.0.0}" --port "${PORT:-8000}"'
