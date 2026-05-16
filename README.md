# shiplogistics - HiveGrid Sample Repository

## 1. Overview

This repository is a fictional but realistic on-premises Hive/Hadoop application
that ingests, aggregates and exports **logistics shipment** data. It runs on
**HiveGrid**, an in-house Hadoop platform that combines Hive SQL transforms,
ksh shell wrappers and an XML-driven Event Engine scheduler. The repo is used
as a fixture to validate an AI-powered migration tool that re-targets on-prem
Hive workloads onto **Google Cloud Platform** (BigQuery + Cloud Composer/Airflow
+ Dataproc Serverless).

## 2. Repository Structure

```
.
|-- Deploy.Config                              # App metadata (jobs, DB names, queue, owner)
|-- .cicd/
|   `-- buildblocks.yaml                       # BuildBlocks CI/CD pipeline definition
|-- event_engine/                              # XML schedules (one per job)
|   |-- daily_shipment_ingest_schedule.xml
|   |-- weekly_dim_refresh_schedule.xml
|   |-- daily_shipment_summary_schedule.xml
|   `-- monthly_shipment_export_schedule.xml
|-- corp/analytics/shiplogistics/app/
|   |-- common/scripts/common_utils.sh         # Notify(), Warn(), log_msg(), count_check()
|   |-- daily_shipment_ingest/                 # Job 1
|   |   |-- config/  (dir.config, filters.config)
|   |   |-- hdl/     (src_shipments, src_shipment_events, stg_shipments)
|   |   |-- hql/     (_load.hql, _check.hql)
|   |   `-- scripts/ (_trigger.sh)
|   |-- weekly_dim_refresh/                    # Job 2
|   |   |-- config/  (dir.config, filters.config)
|   |   |-- hdl/     (src_carrier_master, src_route_master, dim_carrier, dim_route)
|   |   |-- hql/     (_backup.hql, _load.hql, _check.hql)
|   |   `-- scripts/ (_trigger.sh)
|   |-- daily_shipment_summary/                # Job 3
|   |   |-- config/  (dir.config, filters.config)
|   |   |-- hdl/     (shipment_summary, shipment_dq_errors)
|   |   |-- hql/     (_load.hql, _dq.hql, _check.hql)
|   |   `-- scripts/ (_trigger.sh)
|   `-- monthly_shipment_export/               # Job 4
|       |-- config/  (dir.config)
|       |-- hdl/     (export_log)
|       `-- scripts/ (_trigger.sh, monthly_shipment_export.py)
|-- tests/
|   |-- conftest.py                            # Spark + sample-data fixtures
|   |-- test_daily_shipment_ingest.py
|   |-- test_weekly_dim_refresh.py
|   |-- test_daily_shipment_summary.py
|   |-- test_monthly_shipment_export.py
|   `-- requirements-test.txt
|-- README.md
`-- TERM_GLOSSARY.md
```

## 3. Database / Schema Design

### `hivesrcdb` (source landing layer)

| Table | Partition | Description |
|-------|-----------|-------------|
| `src_shipments` | `trans_dt STRING` | Raw shipment header records from the OMS daily export |
| `src_shipment_events` | `event_dt STRING` | Per-shipment tracking scan events from carrier handoff |
| `src_carrier_master` | _unpartitioned_ | Carrier master record set, weekly export |
| `src_route_master` | _unpartitioned_ | Route master record set, weekly export |

### `shiplogistics_db` (use-case layer)

| Table | Partition | Description |
|-------|-----------|-------------|
| `stg_shipments` | `trans_dt STRING` | Cleansed, filtered daily staging table (Job 1 output) |
| `shipment_summary` | `report_month STRING` | Daily aggregated KPIs by carrier/route/service (Job 3 output) |
| `shipment_dq_errors` | `trans_dt STRING` | DQ failures detected during Job 3 |
| `export_log` | _unpartitioned_ | One audit row per Job 4 attempt |

### `shiplogistics_dim_db` (dimension layer)

| Table | Partition | Description |
|-------|-----------|-------------|
| `dim_carrier` | _unpartitioned_ | Active-only carrier dimension (Job 2 output) |
| `dim_route` | _unpartitioned_ | Active-only route dimension (Job 2 output) |
| `dim_carrier_bkp` | _unpartitioned_ | CTAS snapshot of dim_carrier taken before each weekly refresh |
| `dim_route_bkp` | _unpartitioned_ | CTAS snapshot of dim_route taken before each weekly refresh |

## 4. Job Descriptions and Lineage

### 4.1 Job 1 - `daily_shipment_ingest`

| Field | Value |
|-------|-------|
| Schedule | `15 4 * * 1-6` (Mon-Sat 04:15 ET) |
| Trigger  | [daily_shipment_ingest_trigger.sh](corp/analytics/shiplogistics/app/daily_shipment_ingest/scripts/daily_shipment_ingest_trigger.sh) |
| Upstream dependency | `OMS_SHIPMENT_EXPORT_COMPLETE` |
| Published event | `SHIPLOGISTICS_STG_SHIPMENTS_READY` |

**Steps**

1. Source count check on `hivesrcdb.src_shipments` for `trans_dt` (abort if 0).
2. Run dedup pre-check HQL; abort if any duplicate `(shipment_id, trans_dt)` pairs exist.
3. `hive-runner -i filters.config -d ... -f _load.hql` writes `shiplogistics_db.stg_shipments` for the day.
4. Post-load count check on the new partition (abort if 0).

**Filter set**

* `record_type <> 'TEST'`
* `record_type <> 'INTERNAL'`
* `shipment_status_code IN ('BOOKED','IN_TRANSIT','DELIVERED')`
* `declared_value >= 10.00`
* `carrier_id IS NOT NULL`

**Files**

| Path | Role |
|------|------|
| `config/daily_shipment_ingest_dir.config` | Paths, DB names, date vars |
| `config/daily_shipment_ingest_filters.config` | `set hivevar:` filter set |
| `hdl/src_shipments.hdl` | Source DDL |
| `hdl/src_shipment_events.hdl` | Source DDL |
| `hdl/stg_shipments.hdl` | Target DDL |
| `hql/daily_shipment_ingest_check.hql` | Dedup pre-check |
| `hql/daily_shipment_ingest_load.hql` | Main load |
| `scripts/daily_shipment_ingest_trigger.sh` | ksh wrapper |

**Lineage**

```
hivesrcdb.src_shipments       --+
hivesrcdb.src_shipment_events --+--> daily_shipment_ingest_load.hql --> shiplogistics_db.stg_shipments
                                              |
                                              `-- via /corp/platform/hive/bin/hive-runner
```

### 4.2 Job 2 - `weekly_dim_refresh`

| Field | Value |
|-------|-------|
| Schedule | `0 2 * * 0` (Sunday 02:00 ET) |
| Trigger  | [weekly_dim_refresh_trigger.sh](corp/analytics/shiplogistics/app/weekly_dim_refresh/scripts/weekly_dim_refresh_trigger.sh) |
| Upstream dependency | `OMS_MASTER_EXPORT_COMPLETE` |
| Published event | `SHIPLOGISTICS_DIMS_REFRESHED` |

**Steps**

1. Combined active-record count on both master sources (abort if 0).
2. Snapshot `dim_carrier` and `dim_route` to `_bkp` tables via CTAS.
3. Full-refresh both dim tables (`INSERT OVERWRITE`).
4. Post-load count validation on both target tables.

**Active-record rule**

```sql
status = 'A'
AND eff_dt <= load_dt
AND (exp_dt IS NULL OR exp_dt = '' OR exp_dt >= load_dt)
```

**Files**

| Path | Role |
|------|------|
| `config/weekly_dim_refresh_dir.config` | Paths, DB names, date vars |
| `config/weekly_dim_refresh_filters.config` | Active-record hivevars |
| `hdl/src_carrier_master.hdl` | Source DDL |
| `hdl/src_route_master.hdl` | Source DDL |
| `hdl/dim_carrier.hdl` | Target DDL |
| `hdl/dim_route.hdl` | Target DDL |
| `hql/weekly_dim_refresh_check.hql` | Combined source-count check |
| `hql/weekly_dim_refresh_backup.hql` | CTAS snapshot |
| `hql/weekly_dim_refresh_load.hql` | Full refresh |
| `scripts/weekly_dim_refresh_trigger.sh` | ksh wrapper |

**Lineage**

```
hivesrcdb.src_carrier_master --+--> _check.hql ----------> (count gate)
hivesrcdb.src_route_master   --+
                                       |
                                       +-> _backup.hql --> dim_carrier_bkp, dim_route_bkp
                                       |
                                       `-> _load.hql ----> shiplogistics_dim_db.dim_carrier
                                                           shiplogistics_dim_db.dim_route
```

### 4.3 Job 3 - `daily_shipment_summary`

| Field | Value |
|-------|-------|
| Schedule | `30 5 * * 1-6` (Mon-Sat 05:30 ET) |
| Trigger  | [daily_shipment_summary_trigger.sh](corp/analytics/shiplogistics/app/daily_shipment_summary/scripts/daily_shipment_summary_trigger.sh) |
| Upstream dependency | `SHIPLOGISTICS_STG_SHIPMENTS_READY` |
| Published event | `SHIPLOGISTICS_SUMMARY_READY` |

**Steps**

1. Verify `stg_shipments[trans_dt]` is populated.
2. Run the DQ HQL: LEFT JOIN staging with both dims and flag
   `ERR_NO_DIM_CARRIER`, `ERR_NO_DIM_ROUTE`, `ERR_NEG_VALUE`,
   `ERR_NULL_MANDATORY` into `shipment_dq_errors`.
3. Run the summary HQL: INNER JOIN staging with both dims and GROUP BY.
4. Post-load validation on both target tables.
5. Compute integer error rate `err_cnt*100/stg_cnt`; send a WARN
   email (non-fatal) when above `err_rate_threshold` (default 5%).

**Summary columns produced**

`total_count`, `approved_count` (DELIVERED), `rejected_count`
(CANCELLED + RETURNED), `total_amount`, `total_weight_kg`,
`approval_rate` (percentage string), `load_timestamp`.

**Files**

| Path | Role |
|------|------|
| `config/daily_shipment_summary_dir.config` | Paths, DB names, threshold |
| `config/daily_shipment_summary_filters.config` | Scope-control hivevars |
| `hdl/shipment_summary.hdl` | Target DDL |
| `hdl/shipment_dq_errors.hdl` | Target DDL |
| `hql/daily_shipment_summary_check.hql` | Staging partition check |
| `hql/daily_shipment_summary_dq.hql` | DQ error detail |
| `hql/daily_shipment_summary_load.hql` | Aggregated summary |
| `scripts/daily_shipment_summary_trigger.sh` | ksh wrapper |

**Lineage**

```
shiplogistics_db.stg_shipments --+
shiplogistics_dim_db.dim_carrier+--+--> _dq.hql   --> shiplogistics_db.shipment_dq_errors
shiplogistics_dim_db.dim_route  --+--> _load.hql --> shiplogistics_db.shipment_summary
                                                              |
                                                              `-- (err_rate gate -> WARN email)
```

### 4.4 Job 4 - `monthly_shipment_export`

| Field | Value |
|-------|-------|
| Schedule | `0 7 1 * *` (1st of the month, 07:00 ET) |
| Trigger  | [monthly_shipment_export_trigger.sh](corp/analytics/shiplogistics/app/monthly_shipment_export/scripts/monthly_shipment_export_trigger.sh) |
| PySpark  | [monthly_shipment_export.py](corp/analytics/shiplogistics/app/monthly_shipment_export/scripts/monthly_shipment_export.py) |
| Upstream dependency | `SHIPLOGISTICS_SUMMARY_READY` |
| Published event | `SHIPLOGISTICS_MONTHLY_EXPORT_COMPLETE` |

**Steps**

1. Pre-export count check: `shipment_summary` has rows for
   `report_date LIKE 'YYYY-MM-%'` (abort if 0).
2. `spark-submit monthly_shipment_export.py` writes the CSV file.
3. Validate the file exists, has more than HEADER+TRAILER lines, and that
   the last line begins with `TRAILER,`.
4. Copy the file to `/corp/sft/outbound/shiplogistics/`.

**PySpark behavior**

* SparkSession with `enableHiveSupport()`.
* Filter on `report_date LIKE '${export_year_month}-%'`.
* Write CSV with column-name HEADER row and a `TRAILER,<record_count>` footer.
* Write one row to `shiplogistics_db.export_log` with `status='SUCCESS'`
  on success or `status='FAILED'` + truncated stack trace on error.

**Files**

| Path | Role |
|------|------|
| `config/monthly_shipment_export_dir.config` | Paths, output names, spark conf |
| `hdl/export_log.hdl` | Audit log DDL |
| `scripts/monthly_shipment_export.py` | PySpark export |
| `scripts/monthly_shipment_export_trigger.sh` | ksh wrapper |

**Lineage**

```
shiplogistics_db.shipment_summary --+
                                    +--> spark-submit monthly_shipment_export.py
                                    |       |--> shiplogistics_summary_YYYY-MM.csv (HEADER + data + TRAILER)
                                    |       `--> shiplogistics_db.export_log (audit row)
                                    `--> cp .../shiplogistics_summary_YYYY-MM.csv
                                            -> /corp/sft/outbound/shiplogistics/
```

## 5. Key Configuration Concepts

### 5.1 `hive-runner` usage

The on-prem CLI is `/corp/platform/hive/bin/hive-runner`. The three relevant
flags used by every job:

| Flag | Purpose |
|------|---------|
| `-i <file>` | Include file: each `set hivevar:<name>=<value>;` line creates a Hive variable |
| `-d name=value` | Single-variable substitution at invocation time |
| `-f <file.hql>` | Run the HQL script |

A typical call:

```bash
/corp/platform/hive/bin/hive-runner \
  -i ${ConfigDir}/${system}_filters.config \
  -d cs_db=${cs_db} \
  -d uc_db=${uc_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -f ${HqlDir}/${system}_load.hql
```

### 5.2 Config-file injection

Each job has a paired `*_dir.config` (paths and date variables, sourced into
the shell) and a `*_filters.config` (Hive `set hivevar:` lines, passed via
`hive-runner -i`). Filters are kept declarative so business owners can change
scope without code edits.

### 5.3 Date variables

All trigger scripts use Linux `date -d` syntax to compute the substitution
values. Two patterns:

* `trans_dt=$(date -d "-1 day" +"%Y-%m-%d")` - the day a daily job processes.
* `export_year_month=$(date -d "-1 month" +"%Y-%m")` - the previous month
  for monthly export.

The values are passed into HQL via `-d` (where they become `${trans_dt}`
inside the HQL).

## 6. Unit Tests

The `tests/` directory contains pytest cases that re-implement the filter,
dedup, join and aggregation logic in PySpark - they do **not** read the
HQL files. Sample data is constructed in `conftest.py` with deliberately
crafted rows that exercise every filter and DQ branch.

Run the suite:

```bash
pip install -r tests/requirements-test.txt
pytest tests/ -v
```

| File | Covers |
|------|--------|
| [test_daily_shipment_ingest.py](tests/test_daily_shipment_ingest.py) | Filter exclusion (TEST, INTERNAL, CANCELLED, below threshold), dedup detection on clean and polluted input, post-filter row count |
| [test_weekly_dim_refresh.py](tests/test_weekly_dim_refresh.py) | Active-record filter, future-dated exclusion, expired exclusion, backup completeness (rows and schema) |
| [test_daily_shipment_summary.py](tests/test_daily_shipment_summary.py) | Aggregation counts (total / approved / rejected), DQ error row identification per error code, approval rate math, integer error-rate threshold below/above warn |
| [test_monthly_shipment_export.py](tests/test_monthly_shipment_export.py) | Month filter (`LIKE 'YYYY-MM-%'`), CSV HEADER/TRAILER framing, None-to-empty-string conversion, audit log schema match against `export_log.hdl` |

## 7. Migration Notes - On-Prem to GCP Mapping

| On-prem (HiveGrid) | GCP equivalent |
|--------------------|----------------|
| Event Engine XML schedule (`event_engine/*.xml`) | Cloud Composer / Airflow DAG (one `DAG` per file) |
| `.hql` file run via `hive-runner -f` | BigQuery SQL run via `BigQueryInsertJobOperator` (or as a stored query) |
| `hive-runner -i filters.config` (hivevars) | Airflow `params=` or Jinja-templated `query_parameters` on the operator |
| `hive-runner -d name=value` substitution | Jinja templating in BigQuery SQL: `{{ params.trans_dt }}` |
| `hive -e "SELECT COUNT(*) ..."` inline count check | `BigQueryGetDataOperator` (or `BigQueryCheckOperator` for boolean gates) |
| `mail -s` failure email | `EmailOperator` (or `on_failure_callback` -> SendGrid / SMTP hook) |
| Non-fatal `mail -s "WARN"` | Airflow task with `trigger_rule='all_done'` + `EmailOperator` |
| `CREATE EXTERNAL TABLE ... STORED AS TEXTFILE LOCATION 'hdfs:///...'` | BigQuery external table over GCS (`gs://...`) or native BigQuery table (preferred for analytic workloads) |
| Hive partition `PARTITIONED BY (trans_dt STRING)` | BigQuery partitioned table on `trans_dt` (DATE) with require_partition_filter |
| `INSERT OVERWRITE TABLE` | BigQuery `MERGE` or `WRITE_TRUNCATE` job destination |
| `spark-submit` PySpark job | `DataprocSubmitJobOperator` (PySpark on Dataproc Serverless) |
| `SparkSession.builder.enableHiveSupport()` | `SparkSession` reading BigQuery via `spark-bigquery-connector` |
| `cp` to SFT outbound directory | Upload to GCS bucket via `LocalFilesystemToGCSOperator` + downstream Cloud Storage Transfer to SFT |
| `*_dir.config` shell-sourced paths | Airflow Variables + GCS-mounted config bucket |
| YARN queue `shiplogistics` | BigQuery reservation / Dataproc cluster pool |
| Service account `svc_shiplogistics` | GCP service account `svc-shiplogistics@<project>.iam.gserviceaccount.com` |
| `[REDACTED_EMAIL_ADDRESS_1]` distribution list | Cloud Logging-based alerting policy notification channel |
