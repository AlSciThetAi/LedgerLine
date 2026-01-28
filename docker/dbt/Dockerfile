FROM python:3.11-slim

ARG DBT_VERSION=1.8.2
RUN pip install --no-cache-dir dbt-postgres==${DBT_VERSION}

WORKDIR /usr/app
ENTRYPOINT ["bash", "-lc"]