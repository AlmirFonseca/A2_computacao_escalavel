# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /events

# Install any needed packages specified in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /producer
COPY webhook.py .
COPY consumer_example.py .

# chmod
# COPY start.sh .
# RUN chmod +x start.sh

# Run producer.py when the container launches
# CMD ["./start.sh"]
