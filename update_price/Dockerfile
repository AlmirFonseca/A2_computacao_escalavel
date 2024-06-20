# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /update_price

# Copy the requirements file into the container
COPY update_prices.py .
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Run the script
CMD ["python", "update_prices.py"]