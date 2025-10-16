FROM apache/airflow:2.10.5

# Improve pip reliability and speed during image builds
ENV PIP_DEFAULT_TIMEOUT=600 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Create models directory and set ownership
USER root
RUN mkdir -p /app/models

# Install curl for Docker healthchecks
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages as airflow user into the image virtualenv
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --default-timeout=600 -r /tmp/requirements.txt

# PATH remains as set by base image (virtualenv active)