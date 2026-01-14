FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for scylla-driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libev-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY chatbot/ ./chatbot/

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "chatbot.app:app", "--host", "0.0.0.0", "--port", "8000"]
