# TASK

Generate a complete, fully functional sample on-premises Hive repository for a fictional use case. The repo represents a real-world ETL and reporting pipeline built on a Hadoop/Hive-based on-prem platform. I will use this repo to test an AI-powered migration tool that converts on-prem Hive code to Google Cloud Platform (GCP). The code must authentically represent on-prem Hive patterns.

---

# BACKGROUND

In a cloud migration project, the on-prem platform runs Hive SQL for data transformations, shell scripts (ksh) as job orchestrators, and an XML-based Event Engine for scheduling. On GCP the equivalent stack is: BigQuery SQL, Airflow DAGs, and Dataproc/Serverless Spark. The migration tool needs a realistic on-prem Hive repo as input.

The repo should have enough variety to test all conversion scenarios:
- Shell-orchestrated Hive SQL jobs (HQL files)
- Shell-orchestrated PySpark jobs
- Config-file-driven variable injection into Hive
- DDL files (table CREATE statements)
- Event Engine XML schedules

---

# PLATFORM CONVENTIONS

Use these fictional but internally consistent names throughout every file:

| Concept | Value |
|---|---|
| Platform name | `HiveGrid` (the on-prem Hadoop/Hive platform) |
| CLI tool | `hive-runner` at `/corp/platform/hive/bin/hive-runner` |
| CLI flags | `-i <config_file>` injects SET hivevar lines, `-d var=value` sets a single variable, `-f <hql_file>` runs an HQL file |
| Source DB | `hivesrcdb` |
| Use-case DB | `<usecase>_db` |
| Dimension DB | `<usecase>_dim_db` |
| Root HDFS path | `/corp/analytics/<usecase>/` |
| CI/CD config | `.cicd/buildblocks.yaml` |
| Service account | `svc_<usecase>` |
| Notification email | `[REDACTED_EMAIL_ADDRESS_1]` |
| Date syntax | `date -d` Linux style |
| YARN queue | `<usecase>` (e.g. `salesanalytics`) |

---

# REPOSITORY STRUCTURE

Generate this exact layout. Replace `<usecase>` with your chosen use-case name.

```
Deploy.Config   # App metadata
.cicd/
  buildblocks.yaml  # CI/CD pipeline config
event_engine/       # XML job schedules (one per job)
  <job1>_schedule.xml
  <job2>_schedule.xml
  <job3>_schedule.xml
  <job4>_schedule.xml
corp/analytics/<usecase>/app/
  common/
    scripts/
      common_utils.sh   # Shared: Notify(), log_msg(), count check
  <job1>/
    config/
      <job1>_dir.config # Paths, DB names, date vars, email
      <job1>_filters.config # SET hivevar:filter_name=... lines
    hdl/
      <source_tables>.hdl   # CREATE EXTERNAL TABLE for source tables
      <target_table>.hdl    # CREATE EXTERNAL TABLE for staging table
    hql/
      <job1>_load.hql       # Main INSERT OVERWRITE TABLE HQL
      <job1>_check.hql      # Pre-check / validation HQL (returns a count)
    scripts/
      <job1>_trigger.sh     # ksh wrapper
  <job2>/ (same sub-structure: config/, hdl/, hql/, scripts/)
  <job3>/ (same sub-structure)
  <job4>/
    config/
      <job4>_dir.config
    hdl/
      export_log.hdl    # Audit log table DDL
    scripts/
      <job4>_trigger.sh # ksh wrapper that calls spark-submit
      <job4>_export.py # PySpark job
tests/
  conftest.py   # Pytest fixtures: SparkSession + sample DataFrames
  test_<job1>.py
  test_<job2>.py
  test_<job3>.py
  test_<job4>.py
  requirements-test.txt pytest>=7.0 pyspark>=3.0
README.md
```

---

# USE CASE SELECTION

Pick any coherent fictional business domain, e.g. retail orders, insurance claims, logistics shipments, telecom events, or hospital records. **Do NOT use payments or card transactions.**

Design 3-4 source tables in `hivesrcdb` and 4-6 target/dimension tables. Make schemas realistic: 10-20 columns each, with a mix of `STRING`, `BIGINT`, and `DECIMAL` types.

---

# JOBS TO IMPLEMENT

## Job 1 - Daily Ingest (runs Mon-Sat, early morning)

**Purpose:** Load and validate the previous day's raw records from `hivesrcdb` into a partitioned staging table in `<usecase>_db`.

**Shell script steps:**
1. Source count check on the `hivesrcdb` source table, abort if zero.
2. Deduplication pre-check HQL, abort if duplicates are found.
3. Run main load HQL via `hive-runner` with filter config injected via `-i`.
4. Post-load count validation, abort if staging table is empty.

**Filters** (in `<job1>_filters.config`, injected as Hive variables): exclude at least two record types, filter by a status/type code, and apply a minimum value threshold.

**Event Engine XML:** CRON daily Mon-Sat, depends on the source-system-export event.

---

## Job 2 - Dimension Refresh (runs weekly, Sunday)

**Purpose:** Full-refresh two dimension tables from source master data.

**Shell script steps:**
1. Check source active record count, abort if zero.
2. Backup current dim tables to `_bkp` tables (separate backup HQL).
3. Full-refresh both dim tables (main load HQL).
4. Post-load count validation.

**Active record filter:** `status='A'`, `eff_dt <= load_dt`, `exp_dt >= load_dt OR exp_dt IS NULL`.

**Event Engine XML:** CRON weekly (Sunday).

---

## Job 3 - Daily Summary + DQ Errors (runs daily after Job 1)

**Purpose:** Join staging with dimension tables to produce an aggregated summary table and flag data quality errors into an error detail table.

**Shell script steps:**
1. Verify the staging partition has data for today's date.
2. Run DQ error HQL: LEFT JOIN staging with dims; flag `ERR_NO_DIM`, negative values, and null mandatory fields. Insert into the error detail table.
3. Run summary HQL: INNER JOIN staging and dims, GROUP BY, compute counts/amounts/rates.
4. Post-load validation on both tables.
5. Compute integer error rate (`err_cnt * 100 / stg_cnt`); send a WARNING email (non-fatal) if above threshold.

**Summary HQL must compute:** `total_count`, `approved_count`, `rejected_count`, `total_amount`, `approval_rate` (as percentage string), and `load_timestamp`.

**Event Engine XML:** CRON daily Mon-Sat, depends on Job 1.

---

## Job 4 - Monthly Export (runs 1st of each month)

**Purpose:** Export the previous month’s summary data via PySpark to a CSV flat file, then copy it to an SFT outbound directory.

**Shell script steps:**
1. Check the summary table has data for the export month.
2. Run `spark-submit` with the PySpark script.
3. Validate the output file exists and has a non-zero line count.
4. Copy the file to the SFT outbound directory.

**PySpark script behavior:**
- Read the summary table via SparkSession with Hive support.
- Filter by `report_date LIKE 'YYYY-MM-%'`.
- Write CSV with a `HEADER` row and a `TRAILER` row: `TRAILER,<record_count>`.
- Write an audit row to `<usecase>_db.export_log` with columns: `job_name`, `run_dt`, `export_month`, `export_file`, `record_cnt`, `status`, `error_message`, `load_timestamp`.

**Event Engine XML:** CRON monthly (1st of month), depends on Job 3.

---

# FILE PATTERNS

## `<job>_dir.config`

Example contents:

```properties
HomeDir=/corp/analytics/<usecase>/app/<job>
CommonHomeDir=/corp/analytics/<usecase>/app/common
ConfigDir=${HomeDir}/config
LogDir=${HomeDir}/logs
ScriptDir=${HomeDir}/scripts
TmpDir=${HomeDir}/tmp
HqlDir=${HomeDir}/hql
system=<job>
system_desc="<Human readable description>"
mapred_qname=<usecase>
cs_db=hivesrcdb
uc_db=<usecase>_db
dim_db=<usecase>_dim_db
HiveDBPath=/corp/analytics/<usecase>/warehouse/<usecase>_db
trans_dt=$(date -d "-1 day" +"%Y-%m-%d")
year_month_day=$(date +"%Y%m%d")
DistributionEmail=[REDACTED_EMAIL_ADDRESS_1]
```

## `<job>_filters.config`

Example contents:

```properties
# Each line injects a Hive variable
set hivevar:filter_name=AND <condition>;
set hivevar:another_filter=AND <condition>;
```

## `.hdl` files (Hive DDL)

Example contents:

```sql
USE ${db_variable};
DROP TABLE IF EXISTS <TABLE_NAME>;
CREATE EXTERNAL TABLE <TABLE_NAME> (
  col1 STRING,
  col2 STRING,
  col3 BIGINT,
  col4 DECIMAL(15,2)
)
COMMENT '<description>'
PARTITIONED BY (partition_col STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/corp/analytics/<usecase>/warehouse/<db>/<table>';
```

## `.hql` files

Example contents:

```sql
USE ${uc_db};
SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;

INSERT OVERWRITE TABLE ${uc_db}.<TABLE> PARTITION (partition_col)
SELECT
  col1,
  col2,
  col3,
  from_unixtime(unix_timestamp()) AS load_timestamp,
  '${partition_value}' AS partition_col
FROM ${cs_db}.<source_table> t
JOIN ${dim_db}.<dim_table> d ON t.key = d.key
WHERE 1=1
  ${hivevar:filter_name}
  ${hivevar:another_filter}
GROUP BY col1, col2;
```

## Shell scripts (`.sh`)

Example contents:

```bash
#!/bin/ksh
# <Job name> - <purpose>
# Version: YYYY-MM-DD Analytics Team Initial release

Notify() {
  echo -e "This is a system generated message. Job ${system} failed. \nStep: ${step}\nLog: ${LogDir}/${system}_${sysdate}.log" \
    | mail -s "${MailSubject}" "${DistributionEmail}"
  exit 1
}

Warn() {
  echo -e "WARNING: ${WarnMessage}" | mail -s "WARN: ${system}" "${DistributionEmail}"
  # does NOT exit; non-fatal
}

system=<job>
ConfigDir=/corp/analytics/<usecase>/app/${system}/config
DistributionEmail=[REDACTED_EMAIL_ADDRESS_1]
logtm="date +%Y-%m-%d.%H:%M:%S"
sysdate=$(date +"%Y%m%d")

if [ ! -f "${ConfigDir}/${system}_dir.config" ]; then
  echo "$(${logtm}) ERROR: Config file not found: ${ConfigDir}/${system}_dir.config"
  MailSubject="FAILED: ${system} Config not found"
  Notify
fi

# Step 1: Source count check
cnt=$(hive -e "set mapred.job.queue.name=${mapred_qname}; SELECT COUNT(*) FROM ${cs_db}.<table> WHERE trans_dt='${trans_dt}';" 2>/dev/null)
if [ "${cnt}" -eq 0 ]; then
  MailSubject="FAILED: ${system} No source records for ${trans_dt}"
  Notify
fi

# Step 2: Run hive-runner
/corp/platform/hive/bin/hive-runner \
  -i ${ConfigDir}/${system}_filters.config \
  -d cs_db=${cs_db} \
  -d uc_db=${uc_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -f ${HqlDir}/<file>.hql >> ${LogDir}/${system}_load_${sysdate}.log 2>&1
if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Load HQL failed"
  Notify
fi
```

## Event Engine XML

Example contents:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<EventEngine>
  <Event>
    <EventId>USECASE_JOBNAME_FREQUENCY</EventId>
    <Description>Human-readable description of what this job does</Description>
    <Schedule>
      <Type>CRON</Type>
      <Expression>0 6 * * 1-6</Expression>
      <TimeZone>America/New_York</TimeZone>
    </Schedule>
    <Action>
      <Type>SHELL</Type>
      <Command>/corp/analytics/<usecase>/app/<job>/scripts/<job>_trigger.sh</Command>
      <RunAsUser>svc_<usecase></RunAsUser>
      <WorkingDir>/corp/analytics/<usecase>/app/<job></WorkingDir>
    </Action>
    <OnFailure>
      <Notify>true</Notify>
      <Email>[REDACTED_EMAIL_ADDRESS_1]</Email>
    </OnFailure>
    <Dependency>UPSTREAM_EVENT_ID</Dependency>
  </Event>
</EventEngine>
```

## PySpark export script example

```python
#!/usr/bin/env python
"""Monthly export of <usecase> summary data to CSV flat file."""

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hive_db", required=True)
    parser.add_argument("--dim_db", required=True)
    parser.add_argument("--export_year_month", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--queue", required=True)
    return parser.parse_args()


def build_spark_session(queue):
    return (
        SparkSession.builder
        .appName("<job>_export")
        .config("spark.yarn.queue", queue)
        .enableHiveSupport()
        .getOrCreate()
    )


def write_audit_log(spark, hive_db, job_name, run_dt, export_month, export_file, record_cnt, status, error_message):
    schema = StructType([
        StructField("job_name", StringType()),
        StructField("run_dt", StringType()),
        StructField("export_month", StringType()),
        StructField("export_file", StringType()),
        StructField("record_cnt", LongType()),
        StructField("status", StringType()),
        StructField("error_message", StringType()),
        StructField("load_timestamp", TimestampType()),
    ])
    row = [(job_name, run_dt, export_month, export_file, record_cnt, status, error_message, datetime.now())]
    spark.createDataFrame(row, schema).write.insertInto(f"{hive_db}.export_log")


def main():
    args = parse_args()
    spark = build_spark_session(args.queue)
    try:
        df = spark.sql(
            f"SELECT * FROM {args.hive_db}.<summary_table> WHERE report_date LIKE '{args.export_year_month}-%'"
        )
        rows = df.collect()
        with open(args.output_path, "w") as f:
            f.write(",".join(df.columns) + "\n")
            for row in rows:
                f.write(",".join(str(v) if v is not None else "" for v in row) + "\n")
            f.write(f"TRAILER,{len(rows)}\n")
        write_audit_log(
            spark,
            args.hive_db,
            "<job>",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            args.export_year_month,
            args.output_path,
            len(rows),
            "SUCCESS",
            "",
        )
    except Exception as e:
        write_audit_log(
            spark,
            args.hive_db,
            "<job>",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            args.export_year_month,
            args.output_path,
            0,
            "FAILED",
            str(e),
        )
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
```

`conftest.py` (pytest)

```python
import pytest
from pyspark.sql import SparkSession

# Sample data as module-level tuples - NOT dicts
# Include rows that SHOULD be filtered, rows that SHOULD produce DQ errors,
# and rows that ARE valid and should pass through cleanly.

SAMPLE_SOURCE_RECORDS = [
    # (id,  type_cd,  status, value,  date, ...)
    ("REC001", "VALID_TYPE", "A", 150.00, "2026-05-14"),  # valid
    ("REC002", "EXCL_TYPE", "A", 200.00, "2026-05-14"),   # filtered: excluded type
    ("REC003", "VALID_TYPE", "C", 100.00, "2026-05-14"),  # filtered: cancelled status
    ("REC004", "VALID_TYPE", "A", 8.00, "2026-05-14"),    # filtered: below min threshold
    ("REC005", "VALID_TYPE", "A", -50.00, "2026-05-14"),  # DQ error: negative value
    ("REC006", "VALID_TYPE", "A", 300.00, "2026-05-14"),  # DQ error: null mandatory field
    ("REC007", "VALID_TYPE", "A", 175.00, "2026-05-14"),  # DQ error: unknown FK
    ("REC001", "VALID_TYPE", "A", 150.00, "2026-05-14"),  # duplicate of REC001
]

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

@pytest.fixture(scope="session")
def source_df(spark):
    return spark.createDataFrame(
        SAMPLE_SOURCE_RECORDS,
        schema=["id", "type_cd", "status", "value", "date"],
    )

@pytest.fixture(scope="session")
def dim1_df(spark):
    return spark.createDataFrame(
        [
            ("REC001", "DIM_A", "A", "2026-05-01"),
            ("REC003", "DIM_C", "A", "2026-05-01"),
            ("REC004", "DIM_D", "A", "2026-05-01"),
        ],
        schema=["id", "dim_type", "status", "eff_dt"],
    )

@pytest.fixture(scope="session")
def dim2_df(spark):
    return spark.createDataFrame(
        [
            ("REC001", "X123", "active"),
            ("REC003", "X456", "active"),
            ("REC004", "X789", "active"),
        ],
        schema=["id", "external_code", "status"],
    )
```

##Test files ('test_<job>.py')

```python
class TestJoblFilterLogic:
  def test_excl_type_filtered(self, source_df): ...
  def test_cancelled_status_filtered(self, source_df): ...
  def test_below_threshold_filtered(self, source_df): ...
  def test_valid_records_pass(self, source_df): ...
  def test_post_filter_count(self, source_df): ...

class TestJob1DedupCheck:
  def test_no_dups_in_clean_data(self, source_df): ...
  def test_detects_added_duplicate(self, source_df, spark): ...
```

Tests should implement filter/join/aggregation logic in Python/PySpark without reading actual files. Use DataFrame assertions for transform output.

**Required test coverage:**
- Job 1: filter excludes expected rows, dedup detection, post-filter count
- Job 2: active-record filter, future-dated exclusion, expired exclusion, backup completeness
- Job 3: aggregation counts, error rows correctly identified, approval rate math, integer error-rate threshold (below and above)
- Job 4: month filter, CSV header/trailer format, null-to-empty-string conversion, audit log schema

# README Sections

Include all of the following:
1. Overview (2-3 sentences): what the repo is, what platform it runs on, GCP migration context
2. Repository Structure: annotated directory tree
3. Database / Schema Design: for each DB, table name, partition column, description
4. Job Descriptions and Lineage: one subsection per job with schedule, trigger file, upstream dependency, numbered steps, filters/rules applied, files table, ASCII lineage diagram
5. Key Configuration Concepts: hive-runner usage, config injection, date variables
6. Unit Tests: how to run, what each test file covers
7. Migration Notes: mapping table of on-prem patterns to GCP equivalents
   - Event Engine XML schedule | Airflow DAG
   - hql file | BigQuery SQL
   - hive-runner call | BigQueryInsertJobOperator
   - hive -e inline count | BigQueryGetDataOperator
   - mail command | EmailOperator
   - CREATE EXTERNAL TABLE | BigQuery table
   - spark-submit | DataprocSubmitJobOperator
   - SFT copy | GCS upload

# Output Instructions

> **IMPORTANT - follow all of these without exception:**
> - Generate ALL files completely; no placeholders like `rest of file`, `# TODO`, or `...`.
> - Every shell script must have at least 4 numbered steps with logging, hive-runner calls, count validations, and failure/warning email notifications.
> - Every `.hql` must include the full SET block and the full INSERT SELECT with all column names written explicitly.
> - Every `.hdl` must include the full column list for every table.
> - Use only fictional data; no real company names, real email addresses, or real employee names.
> - Use consistent variable names, paths, and DB names across all files.
> - Generate `TERM_GLOSSARY.md` documenting: platform name, CLI tool name and path, source DB name, use-case DB names, root HDFS path, CI/CD directory name, and a Quick Cheat Sheet section.
