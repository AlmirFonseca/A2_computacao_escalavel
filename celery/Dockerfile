# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /celery

# Copy the current directory contents into the container at /celery
COPY mock_celery.py .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run celery when the container launches
CMD ["celery", "-A", "mock_celery", "worker", "--loglevel=INFO", "--concurrency=10"]