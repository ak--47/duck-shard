FROM ubuntu:24.04

# Install system dependencies including DuckDB, jq, and Node.js
RUN apt-get update && \
    apt-get install -y curl jq bash wget unzip && \
    # Install Node.js 20 LTS
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    # Install DuckDB (latest version)
    wget -q https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip && \
    unzip -q duckdb_cli-linux-amd64.zip && \
    mv duckdb /usr/local/bin/ && \
    rm duckdb_cli-linux-amd64.zip && \
    # Clean up apt cache
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Verify installations
RUN duckdb --version && jq --version && node --version

WORKDIR /app

# Copy package.json first for better Docker layer caching
COPY package.json ./

# Install Node.js dependencies
RUN npm ci --only=production

# Copy application files
COPY duck-shard.sh ./
COPY server.mjs ./

# Make duck-shard.sh executable
RUN chmod +x ./duck-shard.sh

# Create non-root user for security
RUN groupadd -r duckuser && useradd -r -g duckuser duckuser && \
    chown -R duckuser:duckuser /app

# Switch to non-root user
USER duckuser

# Set environment variables
ENV PORT=8080
ENV NODE_ENV=production

# Health check
# HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
#     CMD curl -f http://localhost:$PORT/health || exit 1

# Expose the port
EXPOSE $PORT

CMD ["node", "server.mjs"]
