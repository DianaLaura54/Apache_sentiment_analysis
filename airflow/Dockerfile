# Base stage: Use a recent stable Airflow image
FROM apache/airflow:2.9.0-python3.11

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow

# Install the Spark provider package and necessary dependencies in a clean, defined manner.
# This ensures that the core Airflow libraries are correctly linked.
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==5.0.0
