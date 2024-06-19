# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /events

# Copy the current directory contents into the container at /producer
COPY webhook.py .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# chmod
# COPY start.sh .
# RUN chmod +x start.sh

# Run producer.py when the container launches
CMD ["python", "-u", "./webhook.py"]
# CMD ["./start.sh"]
