# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for git, dbt, and other tools
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create directories for data and config
RUN mkdir -p /app/data /app/config /app/dbt

# Set environment variables
ENV PYTHONPATH=/app
ENV PREFECT_API_URL=http://host.docker.internal:4200/api

# Expose any ports your application might need (optional)
EXPOSE 8080

# Default command (can be overridden)
CMD ["python", "-m", "prefect", "version"]