# Daily Orders Pipeline: Airflow + PostgreSQL + dbt

A Docker-based data pipeline that loads the previous completed UTC day's orders from PostgreSQL into a staging table, then uses dbt to build and test a daily revenue reporting table.

The staging load is idempotent: rerunning the same processing date replaces that date's data instead of creating duplicates.

## Architecture

```text
PostgreSQL: raw.orders
          |
          v
Airflow: stage_orders
  Transactional delete + insert for the processing date
          |
          v
PostgreSQL: staging.orders
          |
          v
Airflow: build_revenue_and_test
  dbt build --select daily_revenue
          |
          v
PostgreSQL: analytics.daily_revenue
          |
          v
dbt unique / not_null tests
```

For this local demonstration, source, staging, and analytics tables use separate schemas in the same PostgreSQL database. Airflow metadata is stored in a separate database.

## Technology Stack

- Apache Airflow 2.10.5
- PostgreSQL 16
- dbt Core 1.9.0
- dbt PostgreSQL adapter 1.9.0
- Docker and Docker Compose

dbt is installed in a separate virtual environment inside the Airflow image to isolate its dependencies.

## Repository Structure

```text
.
├── docker-compose.yml
├── Dockerfile
├── README.md
├── .gitignore
├── dags/
│   └── orders_daily.py
├── postgres/
│   └── init.sql
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── sources.yml
        ├── daily_revenue.sql
        └── schema.yml
```

## Prerequisites

- Docker Desktop, or Docker Engine with the Compose plugin
- Git
- An internet connection for the initial image build
- Port `8080` available locally

Check your installation:

```bash
docker --version
docker compose version
```

## Getting Started

### 1. Clone the repository

Replace `YOUR_USERNAME` with the repository owner's GitHub username:

```bash
git clone https://github.com/YOUR_USERNAME/orders-pipeline.git
cd orders-pipeline
```

### 2. Start the services

```bash
docker compose up --build -d
```

The first build downloads the required images and Python dependencies.

Check service status:

```bash
docker compose ps -a
```

Expected states:

| Service | Expected state |
|---|---|
| `postgres` | Running and healthy |
| `airflow-init` | Exited with code `0` |
| `airflow-webserver` | Running |
| `airflow-scheduler` | Running |

### 3. Open Airflow

Open:

http://localhost:8080

Local development credentials:

```text
Username: admin
Password: admin
```

### 4. Enable the DAG

Find `orders_daily` and turn its toggle on.

Alternatively:

```bash
docker compose exec airflow-scheduler \
  airflow dags unpause orders_daily
```

With `catchup=False`, Airflow schedules the latest completed daily interval rather than automatically processing every date since the DAG's start date.

Wait for both tasks to succeed:

```text
stage_orders → build_revenue_and_test
```

## Scheduling and Processing Dates

The DAG uses:

- Schedule: `@daily`
- Timezone: UTC
- Automatic historical catchup: disabled
- Maximum active DAG runs: `1`
- Task retries: `2`

The processing window comes from Airflow's `data_interval_start` and `data_interval_end`, not the computer's current time.

For example, a scheduled run after midnight on November 9 processes:

```text
2026-11-08 00:00:00 UTC <= ordered_at < 2026-11-09 00:00:00 UTC
```

Rerunning that same DAG run uses the same interval, even if it is executed on a later date.

## Pipeline Tasks

### 1. `stage_orders`

The task connects to PostgreSQL using the Airflow connection `orders_db`.

Within a single database transaction, it:

1. Deletes existing staging rows for the processing date.
2. Selects source orders within the run's daily interval.
3. Inserts those orders into `staging.orders`.
4. Commits the transaction.

If the load fails, the transaction rolls back, including the delete.

### 2. `build_revenue_and_test`

The task invokes:

```bash
dbt --warn-error build --select daily_revenue
```

Connection and project-path arguments are configured in the DAG.

dbt builds the revenue table and runs the associated tests. A failed model or test causes the Airflow task to fail.

`--warn-error` also prevents an unmatched model selection from silently succeeding with “Nothing to do.” Other dbt warnings are promoted to errors as well.

## Idempotency

The pipeline is designed to support safe retries and reruns.

### Staging table

The daily load uses transactional delete-then-insert:

```sql
DELETE FROM staging.orders
WHERE order_date = :processing_date;

INSERT INTO staging.orders (...)
SELECT ...
FROM raw.orders
WHERE ordered_at >= :interval_start
  AND ordered_at < :interval_end;
```

The date is a logical partition key; this implementation does not use PostgreSQL physical table partitioning.

### Revenue table

The dbt model rebuilds `analytics.daily_revenue` from all data currently in staging.

This preserves reporting for previously staged dates while avoiding duplicate revenue rows.

### Assumptions

Identical outputs require unchanged source data and transformation logic. If source records are corrected or late orders arrive, rerunning refreshes that day's output intentionally.

`max_active_runs=1` serializes runs of this DAG. External writers would require additional coordination.

## Daily Revenue Model

The model aggregates orders by their UTC order date:

```sql
SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(total_amount)::NUMERIC(18, 2) AS total_revenue
FROM {{ source('staging', 'orders') }}
GROUP BY order_date
```

Output table:

```text
analytics.daily_revenue
```

| Column | Description |
|---|---|
| `order_date` | UTC date of the orders |
| `order_count` | Number of staged orders for the date |
| `total_revenue` | Sum of order amounts for the date |

Assumptions:

- All amounts use one reporting currency.
- All source orders contribute to revenue.
- Dates without orders are omitted rather than represented with zero revenue.

## Data Quality Tests

The dbt project includes:

| Column | Tests |
|---|---|
| `order_date` | `unique`, `not_null` |
| `order_count` | `not_null` |
| `total_revenue` | `not_null` |

These tests check key structural constraints. They do not independently verify revenue calculations or guarantee a nonempty table.

## Verify the Results

### Inspect staged orders

```bash
docker compose exec postgres \
  psql -U orders -d orders \
  -c "SELECT * FROM staging.orders ORDER BY order_date, order_id;"
```

### Inspect daily revenue

```bash
docker compose exec postgres \
  psql -U orders -d orders \
  -c "SELECT * FROM analytics.daily_revenue ORDER BY order_date;"
```

The initialization script creates:

- Three orders for the previous UTC day: `100.00`, `50.00`, and `25.00`.
- One order for the initialization day's UTC date: `999.00`.

If the database is initialized and the pipeline runs on the same UTC day, the expected result for yesterday is:

| order_date | order_count | total_revenue |
|---|---:|---:|
| Previous UTC date | 3 | 175.00 |

The `999.00` order is outside that processing interval.

**Note:** Demo data is generated only when PostgreSQL initializes a new data volume. It is not regenerated on every container restart.

### Inspect test results

In Airflow, open the logs for `build_revenue_and_test`.

Confirm that the model and all four tests completed successfully.

## Verify Idempotency

1. Run the DAG successfully.
2. Record the staging rows and revenue output.
3. Open that same DAG run in Airflow.
4. Clear `stage_orders`, including its downstream task.
5. Wait for both tasks to finish again.
6. Run the verification queries again.

With unchanged inputs, the results should remain identical.

Check for duplicate staging keys:

```bash
docker compose exec postgres \
  psql -U orders -d orders \
  -c "
    SELECT order_date, order_id, COUNT(*)
    FROM staging.orders
    GROUP BY order_date, order_id
    HAVING COUNT(*) > 1;
  "
```

Expected: zero rows.

The staging table's composite primary key also prevents duplicate `(order_date, order_id)` values.

## Useful Commands

### List DAGs

```bash
docker compose exec airflow-scheduler \
  airflow dags list
```

### Check DAG import errors

```bash
docker compose exec airflow-scheduler \
  airflow dags list-import-errors
```

### View scheduler logs

```bash
docker compose logs -f airflow-scheduler
```

### List dbt models

```bash
docker compose exec airflow-scheduler \
  /opt/dbt/bin/dbt ls \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt \
  --target-path /tmp/dbt-debug-target \
  --log-path /tmp/dbt-debug-logs \
  --resource-type model
```

### Run dbt manually

Avoid running this concurrently with the Airflow dbt task because both write the same reporting table.

```bash
docker compose exec airflow-scheduler \
  /opt/dbt/bin/dbt --warn-error build \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt \
  --target-path /tmp/dbt-debug-target \
  --log-path /tmp/dbt-debug-logs \
  --select daily_revenue
```

## Troubleshooting

### `analytics.daily_revenue` does not exist

Confirm that:

- `stage_orders` succeeded.
- The SQL model is named `dbt/models/daily_revenue.sql`.
- The dbt profile targets the `orders` database and `analytics` schema.
- dbt actually built the model rather than reporting “Nothing to do.”

Inspect the dbt task logs for the underlying error.

### dbt cannot find `daily_revenue`

dbt normally derives model names from SQL filenames.

List the models using `dbt ls` and confirm that `daily_revenue` is present.

### The DAG is missing

Run the DAG import-error command and confirm that the Python file is in the mounted `dags/` directory.

### Package downloads time out during the build

Check your internet connection and retry:

```bash
docker compose up --build -d
```

The Dockerfile increases pip's network timeout and connection retry settings.

### Demo dates do not match the current day

The seed script only runs when the database volume is created.

Either process the date represented by the seed data or reset the demo environment using the destructive reset command below.

## Stop or Reset the Environment

Stop containers while retaining data:

```bash
docker compose down
```

Delete the containers and project volumes:

```bash
docker compose down -v
```

**Warning:** The second command deletes the demo databases, Airflow metadata, and the named Airflow log volume.

Recreate the environment:

```bash
docker compose up --build -d
```

## Security and Scope

This project is intended for local development and demonstration.

- Credentials in Docker Compose are demo credentials.
- Do not expose the environment publicly with these settings.
- Production deployments should use secret management and restricted database roles.
- The revenue model is fully rebuilt for simplicity; large datasets may require incremental processing.
- Airflow catchup is disabled for convenient local startup. Historical processing must be initiated explicitly or catchup deliberately configured.