#!/usr/bin/env bash
# Setup DB Connection String
AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
export AIRFLOW__CORE__SQL_ALCHEMY_CONN
AIRFLOW__WEBSERVER__SECRET_KEY="openssl rand -hex 30"
export AIRFLOW__WEBSERVER__SECRET_KEY
AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__EXECUTOR
DBT_POSTGRESQL_CONN="postgresql://${DBT_POSTGRES_USER}:${DBT_POSTGRES_PASSWORD}@${DBT_POSTGRES_HOST}:${DBT_POSTGRES_PORT}/${DBT_POSTGRES_DB}"
rm -f /airflow/airflow-webserver.pid

airflow db upgrade
airflow users create -r Admin -u admin -e admin@example.com -f admin -l user -p airflow
airflow connections add --conn-description 'dbt_postgres_instance_raw_data' --conn-uri $DBT_POSTGRESQL_CONN 'dbt_postgres_instance_raw_data'
airflow scheduler & airflow webserver
