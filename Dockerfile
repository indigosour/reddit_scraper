# Use the official Python base image
FROM python:3.10-slim

# Set the working directory
WORKDIR  /app

#Copy source files to the container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Start the worker script when the container is run
CMD ["python", "worker.py"]