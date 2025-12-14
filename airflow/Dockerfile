# Dockerfile for Airflow with LangChain and Sentiment Analysis Dependencies
FROM apache/airflow:2.9.0-python3.11

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root to install system dependencies if needed
USER root

# Install any system-level dependencies (optional)
# RUN apt-get update && apt-get install -y <package-name> && apt-get clean

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
# Using --user flag to install in user directory since we're not root
RUN pip install --no-cache-dir --user -r /requirements.txt

# Alternative: Install packages directly (if no requirements.txt)
# Uncomment this block and comment out the COPY and RUN above if you prefer
# RUN pip install --no-cache-dir --user \
#     apache-airflow-providers-apache-spark==5.0.0 \
#     apache-airflow-providers-snowflake==5.2.1 \
#     langchain==0.1.0 \
#     langchain-anthropic==0.1.1 \
#     langchain-core==0.1.10 \
#     anthropic==0.18.0 \
#     pydantic>=2.0.0 \
#     snowflake-connector-python==3.6.0 \
#     kafka-python==2.0.2 \
#     pandas==2.1.4 \
#     numpy==1.26.3

# Set working directory
WORKDIR /opt/airflow

# Note: Volumes are mounted via docker-compose.yml:
# - ./airflow/dags:/opt/airflow/dags
# - ./airflow/logs:/opt/airflow/logs
# - ./airflow/plugins:/opt/airflow/plugins
# - ./scripts:/opt/airflow/scripts  (NEW for LangChain module)
# - ./data:/opt/airflow/data
