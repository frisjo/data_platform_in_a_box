
# Base image (pre-built operating system)
FROM python:3.12-slim

# Creates a directory inside the container
WORKDIR /opt/dagster/app

# Copies everything in dagster project folder (data_platform)
COPY . .

# Dependencies
RUN pip install .

# Port 3000 inside container
EXPOSE 3000

# Defining default commands, runs when container starts
CMD ["dagster", "dev", "--host", "0.0.0.0"]