# Use a slim Python base image
FROM python:3.12-slim

# Install system dependencies needed for psycopg2
RUN apt-get update && apt-get install -y gcc libpq-dev postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY app ./app
COPY python_scripts ./python_scripts

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose backend port
EXPOSE 8000

# Run entrypoint script (executes your loaders, then starts FastAPI)
CMD ["/entrypoint.sh"]
