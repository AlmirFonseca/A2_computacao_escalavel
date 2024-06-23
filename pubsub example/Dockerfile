# Use an official Python runtime as a parent image
FROM python:3.9-buster

# Set the working directory in the container
WORKDIR /events

# Install any needed packages specified in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /producer
COPY event_publisher.py .
COPY event_subscriber.py .
