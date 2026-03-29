# Use Debian Bookworm (Stable) for reliable Java 17 access
FROM python:3.11-slim-bookworm

# Install Java 17 (Required by modern PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Bookworm + Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set PYTHONPATH to the project root
ENV PYTHONPATH=/usr/src/app

WORKDIR /usr/src/app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Default: start the web server (Railway overrides this per service via Procfile)
# Using shell form to support $PORT expansion
CMD gunicorn --bind 0.0.0.0:${PORT:-8080} --timeout 120 --pythonpath . main:app
