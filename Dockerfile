# Use a STABLE version of Debian (Bullseye) to avoid "trixie" repository issues
FROM python:3.11-bullseye

# Install Java (OpenJDK 11 is the rock-solid default for Spark on Bullseye)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Bullseye + OpenJDK 11
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
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
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--pythonpath", ".", "main:app"]
