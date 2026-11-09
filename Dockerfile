FROM apache/airflow:2.10.5-python3.11

ENV PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

USER root

RUN python -m venv /opt/dbt \
    && /opt/dbt/bin/pip install --no-cache-dir \
        "dbt-core==1.9.0" \
        "dbt-postgres==1.9.0"

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    psycopg2-binary \
    --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"