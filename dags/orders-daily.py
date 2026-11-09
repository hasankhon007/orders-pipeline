import logging
from datetime import timedelta

import pendulum
import psycopg2

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context


def load_orders():
    context = get_current_context()

    interval_start = context["data_interval_start"].in_timezone("UTC")
    interval_end = context["data_interval_end"].in_timezone("UTC")

    processing_date = interval_start.date()

    connection = BaseHook.get_connection("orders_db")

    conn = psycopg2.connect(
        host=connection.host,
        port=connection.port or 5432,
        dbname=connection.schema,
        user=connection.login,
        password=connection.password,
    )

    try:
        # psycopg2 commits on success and rolls back on exception.
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM staging.orders
                    WHERE order_date = %s
                    """,
                    (processing_date,),
                )

                cursor.execute(
                    """
                    INSERT INTO staging.orders (
                        order_id,
                        order_date,
                        ordered_at,
                        total_amount
                    )
                    SELECT
                        order_id,
                        %s::date,
                        ordered_at,
                        total_amount
                    FROM raw.orders
                    WHERE ordered_at >= %s
                      AND ordered_at < %s
                    """,
                    (
                        processing_date,
                        interval_start,
                        interval_end,
                    ),
                )

                inserted_rows = cursor.rowcount

        logging.info(
            "Loaded %s orders for %s",
            inserted_rows,
            processing_date,
        )

    finally:
        conn.close()


with DAG(
    dag_id="orders_daily",
    description="Load daily orders and build tested revenue models",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["orders", "postgres", "dbt"],
) as dag:

    stage_orders = PythonOperator(
        task_id="stage_orders",
        python_callable=load_orders,
    )

    build_revenue = BashOperator(
        task_id="build_revenue_and_test",
        bash_command="""
            set -euo pipefail

            /opt/dbt/bin/dbt build \
                --project-dir /opt/airflow/dbt \
                --profiles-dir /opt/airflow/dbt \
                --target-path /tmp/dbt-target \
                --log-path /tmp/dbt-logs \
                --select daily_revenue
        """,
    )

    stage_orders >> build_revenue