FROM python:3.11-slim

WORKDIR /app

# Install system deps (needed by google-cloud-bigquery C extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source (hot-reloaded via volume mount in dev)
COPY . .

EXPOSE 8502

CMD ["streamlit", "run", "dashboard.py", \
     "--server.port=8502", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.fileWatcherType=poll"]