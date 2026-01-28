FROM python:3.11-slim

ARG SODA_VERSION=3.3.17
RUN pip install --no-cache-dir soda-core==${SODA_VERSION} soda-core-postgres==${SODA_VERSION}

WORKDIR /soda
ENTRYPOINT ["bash", "-lc"]