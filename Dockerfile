# Use the official Python base image
FROM python:3.10

# Set the working directory
WORKDIR  /app

# Install Git
RUN apt-get update && apt-get install -y git

# Clone the GitHub repository
RUN git clone https://github.com/indigosour/reddit_scraper.git /app

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Start the worker script when the container is run
CMD ["python", "worker.py"]