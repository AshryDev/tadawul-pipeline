FROM python:3.10-slim-buster

# Create necessary directories
RUN mkdir -p /usr/dbt /usr/.dbt

# Copy all DBT project files
COPY . /usr/dbt/

# Copy entrypoint script
COPY entrypoint.sh /usr/docker/entrypoint.sh
RUN chmod +x /usr/docker/entrypoint.sh

WORKDIR /usr/dbt

# Install requirements during build (more efficient)
RUN pip install -r requirements.txt

ENTRYPOINT ["/usr/docker/entrypoint.sh"]
