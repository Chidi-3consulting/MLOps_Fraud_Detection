FROM python:3.10.16-slim-bookworm
WORKDIR /app

# Install curl for healthchecks
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install -r /app/requirements.txt

# Copy the rest of the application
COPY . .

# Copy startup scripts
COPY start_mlflow.py /app/start_mlflow.py
RUN chmod +x /app/start_mlflow.py

# Set the command to run your application
EXPOSE 5500