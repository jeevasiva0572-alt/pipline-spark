# Use full Python Bookworm to ensure package availability
FROM python:3.11-bookworm

# Install OpenJDK 17 (standard in Debian Bookworm)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set PySpark to use the system Python
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Default: start the web server (Railway overrides this per service via Procfile)
CMD gunicorn app.main:app --bind 0.0.0.0:$PORT --timeout 120
