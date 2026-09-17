FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip setuptools wheel

COPY dbt_core_experimental_parser-2.0.0b2-py3-none-manylinux_2_28_x86_64.whl /tmp/dbt_core_experimental_parser-2.0.0b2-py3-none-manylinux_2_28_x86_64.whl

RUN pip install --no-cache-dir \
    /tmp/dbt_core_experimental_parser-2.0.0b2-py3-none-manylinux_2_28_x86_64.whl \
    dbt-core==1.12.3 \
    dbt-spark==1.11.0 \
    PyHive==0.7.0 \
    thrift==0.22.0 \
    thrift-sasl==0.4.3 \
    pure-sasl==0.6.2

WORKDIR /dbt_project

CMD ["dbt", "--version"]