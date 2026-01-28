FROM apache/airflow:2.9.3-python3.11

USER root
# For Postgres connectivity and building wheels if needed
RUN apt-get update && apt-get install -y gcc libpq-dev && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
  apache-airflow-providers-docker==3.11.0 \
  psycopg2-binary==2.9.9