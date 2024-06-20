# Use an official Python runtime as a parent image
FROM python:3.9-buster

# Set the working directory in the container
WORKDIR /bonus

# Copy the current directory contents into the container at /bonus
COPY bonus_system.py .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the default command to execute the Python script
CMD ["python", "bonus_system.py"]