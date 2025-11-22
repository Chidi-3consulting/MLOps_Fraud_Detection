FROM python:3.10.16-slim-bookworm
WORKDIR /app

# Install curl and postgresql-client for healthchecks and database operations
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl postgresql-client && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install -r /app/requirements.txt

# Copy entrypoint/start script
COPY start-mlflow.sh /app/start-mlflow.sh
RUN chmod +x /app/start-mlflow.sh

# Set the command to run your application
EXPOSE 5500