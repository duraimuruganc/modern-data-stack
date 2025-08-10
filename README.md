# Modern Data Stack ETL Pipeline  
**Python · Airflow · AWS S3 · Snowflake (Snowpipe) · dbt · Tableau**

This project implements a resume-accurate pipeline:

- **Airflow** orchestrates: API → Parquet → **S3**  
- **Data Quality** Python check before upload  
- **Snowflake Snowpipe** auto-ingests from S3  
- **dbt** builds a tested **staging** model  
- **Tableau** connects live to Snowflake views for charts

---

## Architecture

```
API → Airflow (extract → DQ → upload) → S3 (raw/)
                               │
                               └── Snowpipe → Snowflake RAW.POSTS → dbt STAGING.STG_POSTS → ANALYTICS views → Tableau
```

---

## Prerequisites

- Docker & Docker Compose
- AWS account with an S3 bucket (e.g., `s3://<YOUR_BUCKET>`)
- Snowflake account (role with CREATE privileges)
- Tableau **Desktop** (Creator / trial) for the Snowflake connector

---

## Quick Start

### 1) Configure environment
Copy and fill env vars (do **not** commit secrets):
```bash
cp .env.example .env
```
Required keys (examples):
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-south-1
S3_BUCKET=<YOUR_BUCKET>

SNOWFLAKE_ACCOUNT=<acct>.region  # e.g. xy12345.ap-south-1
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=API_DATA
SNOWFLAKE_SCHEMA=RAW
```

### 2) Start Airflow
```bash
docker compose up -d
```
Airflow UI: http://localhost:8080 (user/pass: `admin`/`admin` in this setup)

### 3) Snowflake setup (one-time)

Run these in a Snowflake worksheet (replace placeholders as needed).

**Stage, file format**
```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS API_DATA;
CREATE SCHEMA IF NOT EXISTS API_DATA.RAW;
CREATE SCHEMA IF NOT EXISTS API_DATA.ANALYTICS;

USE DATABASE API_DATA; USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT ff_parquet TYPE = PARQUET;

CREATE OR REPLACE STAGE s3_raw
  URL='s3://<YOUR_BUCKET>/raw/'
  CREDENTIALS=(AWS_KEY_ID='<AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT=(FORMAT_NAME=ff_parquet);
```

**Table + Snowpipe**
```sql
CREATE OR REPLACE TABLE RAW.POSTS (
  userId NUMBER,
  id     NUMBER,
  title  STRING,
  body   STRING
);

CREATE OR REPLACE PIPE RAW.PIPE_POSTS
  AUTO_INGEST = TRUE
  AS COPY INTO RAW.POSTS
     FROM @s3_raw
     FILE_FORMAT=(FORMAT_NAME=ff_parquet)
     MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
     PATTERN='.*\.parquet';
```

> If you haven’t wired S3 events → Snowpipe yet, you can force-ingest during dev with:  
> `ALTER PIPE RAW.PIPE_POSTS REFRESH;`

### 4) Run the pipeline
1. In Airflow, **unpause** and **Trigger** the DAG `extract_to_s3_dag`  
   (uploads a Parquet to `s3://<bucket>/raw/…`)
2. In Snowflake:
   ```sql
   SELECT COUNT(*) FROM API_DATA.RAW.POSTS;
   ```
   Expect rows to increase. If not using auto-ingest yet, run:
   ```sql
   ALTER PIPE RAW.PIPE_POSTS REFRESH;
   ```

### 5) dbt (staging model + tests)
Run inside the Airflow container (dbt is preinstalled):
```bash
docker compose exec airflow bash -lc "dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
```
- Model: `ANALYTICS_STAGING.STG_POSTS`  
- Tests: `not_null`, `unique` (with dedup logic in the model)

### 6) Tableau (quick chart)
1. Open **Tableau Desktop** → **Connect → Snowflake**
2. Server: `<SNOWFLAKE_ACCOUNT>.snowflakecomputing.com`
3. Warehouse: `COMPUTE_WH`, Database: `API_DATA`, Schema: `ANALYTICS`
4. Use view:
   ```sql
   CREATE OR REPLACE VIEW API_DATA.ANALYTICS.VW_POST_COUNTS_BY_USER AS
   SELECT user_id, COUNT(*) AS post_count FROM API_DATA.ANALYTICS_STAGING.STG_POSTS GROUP BY 1;
   ```
5. Build a bar chart (user_id vs post_count). Optional: add a Z-score calc to highlight outliers.

---

## Repository Structure

```
dags/
  extract_to_s3_dag.py          # Airflow: extract → DQ → upload (Snowpipe loads)
  include/
    extract_to_s3.py            # fetch API → Parquet → S3
    dq.py                       # data-quality checks
dbt/
  dbt_project.yml
  profiles.yml (or use env vars)
  models/
    sources.yml
    staging/
      stg_posts.sql             # casts/cleanup + dedup
      schema.yml                # tests
snowflake/                      # optional helper SQL scripts
docker-compose.yml
.env.example
README.md
```

---

## Data Quality (before load)

`dq.py` checks:
- file readable
- required columns exist: `userId, id, title, body`
- non-empty batch
- no nulls in `id` (and optional per-batch duplicates)

---
**Author:** Durai Murugan
