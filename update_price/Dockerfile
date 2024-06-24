# Use an official Python runtime as a parent image
FROM python:3.9-buster

# SET ENVIRONMENT VARIABLES
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY update_prices.py .
COPY database_secrets.py .
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt
