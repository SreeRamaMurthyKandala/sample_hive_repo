# TERM_GLOSSARY.md - shiplogistics On-Prem Repository

Reference for every named concept and path used across the repo. Both the
migration tool and human reviewers should be able to resolve any abbreviation
or path on this page alone.

## Core platform terms

| Term | Definition |
|------|------------|
| **HiveGrid** | The fictional on-prem Hadoop/Hive platform that hosts this app. Provides Hive, YARN, HDFS and the Event Engine scheduler. |
| **Event Engine** | XML-driven scheduler equivalent to cron + DAG-style dependencies. One XML file per job under `event_engine/`. |
| **BuildBlocks** | Corporate CI/CD runner. Reads `.cicd/buildblocks.yaml` and executes the declared `blocks` in order, gated per environment. |
| **hive-runner** | The on-prem CLI wrapper that submits HQL files to HiveServer2 with hivevar substitution. |

## CLI tool

| Item | Value |
|------|-------|
| Tool name | `hive-runner` |
| Absolute path | `/corp/platform/hive/bin/hive-runner` |
| Include flag | `-i <config_file>` (file contains `set hivevar:name=value;` lines) |
| Define flag | `-d name=value` (single hivevar at invocation time) |
| Script flag | `-f <hql_file>` (HQL to execute) |

## Database names

| Logical role | Hive DB name | Description |
|--------------|--------------|-------------|
| Source landing | `hivesrcdb` | Raw external tables over upstream drops |
| Use-case | `shiplogistics_db` | Stagings, aggregates, DQ errors, audit |
| Dimension | `shiplogistics_dim_db` | Active-only dimension tables and `_bkp` snapshots |

## HDFS paths

| Path | Purpose |
|------|---------|
| `/corp/analytics/shiplogistics/` | Root path for the application on HDFS |
| `/corp/analytics/shiplogistics/landing/hivesrcdb/` | External-table `LOCATION` for source drops |
| `/corp/analytics/shiplogistics/warehouse/shiplogistics_db/` | External-table `LOCATION` for use-case tables |
| `/corp/analytics/shiplogistics/warehouse/shiplogistics_dim_db/` | External-table `LOCATION` for dim tables |
| `/corp/analytics/shiplogistics/app/<job>/` | Per-job home directory containing `config/`, `hdl/`, `hql/`, `scripts/`, `logs/`, `tmp/` |
| `/corp/sft/outbound/shiplogistics/` | SFT outbound directory for monthly CSV exports |

## Repository directories

| Directory | Role |
|-----------|------|
| `.cicd/` | CI/CD configuration directory consumed by BuildBlocks |
| `event_engine/` | XML schedule files registered with Event Engine |
| `corp/analytics/shiplogistics/app/common/` | Shared shell helpers (`common_utils.sh`) |
| `corp/analytics/shiplogistics/app/<job>/config/` | Per-job `_dir.config` + `_filters.config` |
| `corp/analytics/shiplogistics/app/<job>/hdl/` | Hive DDL files (CREATE EXTERNAL TABLE) |
| `corp/analytics/shiplogistics/app/<job>/hql/` | Job transform / check HQL scripts |
| `corp/analytics/shiplogistics/app/<job>/scripts/` | ksh trigger scripts and PySpark scripts |
| `tests/` | PySpark unit tests against in-memory sample data |

## Operational identities

| Concept | Value |
|---------|-------|
| Service account | `svc_shiplogistics` (runs every cron-driven trigger script) |
| YARN queue | `shiplogistics` |
| Notification email | `[REDACTED_EMAIL_ADDRESS_1]` |
| Timezone for schedules | `America/New_York` |

## Naming conventions

| Pattern | Meaning |
|---------|---------|
| `src_<name>` | External table over an upstream landing drop in `hivesrcdb` |
| `stg_<name>` | Cleansed, filtered partitioned staging table in `<usecase>_db` |
| `dim_<name>` | Conformed dimension in `<usecase>_dim_db` |
| `dim_<name>_bkp` | CTAS snapshot of a dim taken pre-refresh for rollback |
| `<table>_dq_errors` | DQ error detail table with `error_code` discriminator |
| `<file>_dir.config` | Shell-sourced KV file: paths, DB names, date vars |
| `<file>_filters.config` | `set hivevar:...;` lines, passed via `hive-runner -i` |
| `<job>_trigger.sh` | ksh entry point referenced by the Event Engine XML |

## Date variables

| Variable | Computation | Used by |
|----------|-------------|---------|
| `trans_dt` | `$(date -d "-1 day" +"%Y-%m-%d")` | Daily jobs (process previous day) |
| `load_dt` | `$(date +"%Y-%m-%d")` | Weekly dim refresh (validate "active as of today") |
| `report_month` | `$(date -d "-1 day" +"%Y-%m")` | Daily summary partition column |
| `export_year_month` | `$(date -d "-1 month" +"%Y-%m")` | Monthly export (export previous month) |
| `year_month_day` | `$(date +"%Y%m%d")` | Log file naming |

## DQ error codes (Job 3)

| Code | Trigger condition |
|------|-------------------|
| `ERR_NO_DIM_CARRIER` | `carrier_id` not present in `dim_carrier` |
| `ERR_NO_DIM_ROUTE` | `route_id` not present in `dim_route` |
| `ERR_NEG_VALUE` | `declared_value < 0` OR `shipping_charge < 0` |
| `ERR_NULL_MANDATORY` | Any of `tracking_number`, `origin_facility_cd`, `destination_facility_cd`, `weight_kg` is NULL/empty |

## Common shell helper functions

Sourced from `corp/analytics/shiplogistics/app/common/scripts/common_utils.sh`.

| Function | Behavior |
|----------|----------|
| `log_msg <severity> <msg>` | Timestamped log line to STDOUT (redirected to the per-run log) |
| `Notify` | FATAL: sends email with subject `${MailSubject}`, exits 1 |
| `Warn` | NON-FATAL: sends email with subject `${WarnSubject}`, does not exit |
| `count_check <cnt> <label>` | Calls `Notify` if count is missing, non-numeric, zero or negative |
| `run_hive_count <hql>` | Runs an inline `hive -e` query and prints the integer count |
| `require_file <path>` | Calls `Notify` if file is missing |
| `load_dir_config <path>` | `require_file` + `. <path>` to source a `_dir.config` |

## Quick Cheat Sheet

```bash
# 1) Run a single HQL script against a target DB with a filter file
/corp/platform/hive/bin/hive-runner \
  -i ${ConfigDir}/${system}_filters.config \
  -d uc_db=shiplogistics_db \
  -d mapred_qname=shiplogistics \
  -d trans_dt="2026-05-14" \
  -f ${HqlDir}/${system}_load.hql

# 2) Inline count check (used by trigger scripts)
hive -e "set mapred.job.queue.name=shiplogistics;
         SELECT COUNT(*) FROM shiplogistics_db.stg_shipments
         WHERE trans_dt='2026-05-14';" 2>/dev/null | tail -1

# 3) Manually trigger any job (Event Engine equivalent)
sudo -u svc_shiplogistics \
  /corp/analytics/shiplogistics/app/daily_shipment_ingest/scripts/daily_shipment_ingest_trigger.sh

# 4) spark-submit the monthly export
/corp/platform/spark/bin/spark-submit \
  --master yarn --deploy-mode client --queue shiplogistics \
  /corp/analytics/shiplogistics/app/monthly_shipment_export/scripts/monthly_shipment_export.py \
    --hive_db shiplogistics_db \
    --dim_db  shiplogistics_dim_db \
    --export_year_month 2026-04 \
    --output_path /corp/analytics/shiplogistics/app/monthly_shipment_export/outbound/shiplogistics_summary_2026-04.csv \
    --queue shiplogistics \
    --job_name monthly_shipment_export

# 5) Re-register an Event Engine schedule after a change
eventctl register event_engine/daily_shipment_ingest_schedule.xml

# 6) Run the pytest suite locally
pip install -r tests/requirements-test.txt
pytest tests/ -v
```
