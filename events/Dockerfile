# Use an official Python runtime as a parent image
FROM python:3.9-buster

# SET ENVIRONMENT VARIABLES
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /events

# INSTALL OPENJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre && \
    apt-get clean;

# Install any needed packages specified in requirements.txt
COPY requirements.txt .

# INSTALL SYSTEM DEPENDENCIES
RUN set -ex \
    && apt-get update -qq \
    && apt-get install -y libpq-dev \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /root/.cache \
    && find / -name "*.pyc" -exec rm -f {} \;

# Copy the current directory contents into the container at /producer
COPY webhook.py .
COPY queue_consumer.py .
