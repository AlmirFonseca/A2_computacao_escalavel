# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /consumer

# Copy the current directory contents into the container at /consumer
COPY consumer.py .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# chmod
# COPY start.sh .
# RUN chmod +x start.sh

# Run consumer.py when the container launches
CMD ["python", "./consumer.py"]
# CMD ["./start.sh"]