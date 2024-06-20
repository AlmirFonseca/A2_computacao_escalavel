# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /price_monitor

# Copy the current directory contents into the container at /price_monitor
COPY price_monitor.py .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the default command to execute the Python script
CMD ["python", "price_monitor.py"]