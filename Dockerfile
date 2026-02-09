# Dockerfile
FROM apache/airflow:3.1.1

# Copy requirements.txt into image
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt
