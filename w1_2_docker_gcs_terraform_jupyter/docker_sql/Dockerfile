FROM python:3.9.1

# Update package lists and install wget with the -y flag
RUN apt-get update && apt-get install -y wget

# Install Python packages
RUN pip install pandas sqlalchemy psycopg2

# Set the working directory to /app
WORKDIR /app

# Copy the ingest_data.py script into the container
COPY ingest_data.py ingest_data.py

# Specify the entry point for running the script
ENTRYPOINT [ "python", "ingest_data.py" ]
