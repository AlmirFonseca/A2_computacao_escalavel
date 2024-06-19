# Pyython base image
FROM python:3.9-buster

# SET ENVIRONMENT VARIABLES
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# SET WORKDIR
WORKDIR /processor

# INSTALL OPENJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre && \
    apt-get clean;

# Get postgresql driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# COPY PYTHON DEPENDENCIES
COPY ./requirements.txt .

# INSTALL SYSTEM DEPENDENCIES
RUN set -ex \
    && apt-get update -qq \
    && apt-get install -y libpq-dev \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /root/.cache \
    && find / -name "*.pyc" -exec rm -f {} \;


# COPY APPLICATION
COPY ./data_processor.py .