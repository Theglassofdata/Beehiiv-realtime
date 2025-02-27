#!/bin/bash
set -e

# Install dependencies from requirements.txt
if [ -e "/opt/airflow/requirements.txt" ]; then
  pip install --upgrade pip
  pip install --no-cache-dir --user -r /opt/airflow/requirements.txt
fi

# Initialize Airflow DB if running the webserver and DB is not initialized
if [ "$1" = "webserver" ] && [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the database schema
airflow db upgrade

# Execute the Airflow command (webserver or scheduler)
exec airflow "$@"