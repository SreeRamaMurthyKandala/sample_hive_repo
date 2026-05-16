#!/usr/bin/env python
"""Monthly export of shiplogistics shipment_summary data to a CSV flat file.

Reads ${uc_db}.shipment_summary via a SparkSession with Hive support, filters
to the requested year-month, writes a CSV with a HEADER row and a
TRAILER,<record_count> footer line, then writes a single audit row into
${uc_db}.export_log capturing SUCCESS or FAILED status.

Invoked by monthly_shipment_export_trigger.sh as:

    spark-submit monthly_shipment_export.py \\
        --hive_db shiplogistics_db \\
        --dim_db shiplogistics_dim_db \\
        --export_year_month 2026-04 \\
        --output_path /corp/analytics/.../shiplogistics_summary_2026-04.csv \\
        --queue shiplogistics \\
        --job_name monthly_shipment_export
"""

import argparse
import sys
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Columns written into the CSV header (and consumed by downstream partners).
EXPORT_COLUMNS = [
    "report_date",
    "report_month",
    "carrier_id",
    "carrier_name",
    "route_id",
    "region_cd",
    "transit_mode",
    "service_level",
    "total_count",
    "approved_count",
    "rejected_count",
    "total_amount",
    "total_weight_kg",
    "approval_rate",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Monthly shiplogistics export")
    parser.add_argument("--hive_db", required=True,
                        help="Use-case Hive database (e.g. shiplogistics_db)")
    parser.add_argument("--dim_db", required=True,
                        help="Dimension Hive database (e.g. shiplogistics_dim_db)")
    parser.add_argument("--export_year_month", required=True,
                        help="Year-month to export, format YYYY-MM")
    parser.add_argument("--output_path", required=True,
                        help="Absolute local filesystem path for the CSV file")
    parser.add_argument("--queue", required=True,
                        help="YARN queue name")
    parser.add_argument("--job_name", required=True,
                        help="Logical job name written into export_log.job_name")
    return parser.parse_args()


def build_spark_session(queue, job_name):
    return (
        SparkSession.builder
        .appName(job_name)
        .config("spark.yarn.queue", queue)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )


def fetch_summary(spark, hive_db, export_year_month):
    """Return the filtered summary rows as a list of Row objects."""
    query = (
        "SELECT "
        + ", ".join(EXPORT_COLUMNS)
        + f" FROM {hive_db}.shipment_summary"
        f" WHERE report_date LIKE '{export_year_month}-%'"
        " ORDER BY report_date, carrier_id, route_id, service_level"
    )
    df = spark.sql(query)
    return df, df.collect()


def render_cell(value):
    """Convert any cell value into the CSV string the partners expect.

    None becomes the empty string. Everything else is str()'d.
    """
    if value is None:
        return ""
    return str(value)


def write_csv(output_path, columns, rows):
    """Write the CSV with HEADER + data rows + TRAILER,<count>."""
    with open(output_path, "w") as fh:
        fh.write(",".join(columns) + "\n")
        for row in rows:
            cells = [render_cell(row[col]) for col in columns]
            fh.write(",".join(cells) + "\n")
        fh.write(f"TRAILER,{len(rows)}\n")


def write_audit_log(spark, hive_db, job_name, run_dt, export_month,
                    export_file, record_cnt, status, error_message):
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
    row = [(
        job_name,
        run_dt,
        export_month,
        export_file,
        int(record_cnt),
        status,
        error_message,
        datetime.now(),
    )]
    audit_df = spark.createDataFrame(row, schema)
    audit_df.write.insertInto(f"{hive_db}.export_log")


def main():
    args = parse_args()
    spark = build_spark_session(args.queue, args.job_name)
    run_dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        df, rows = fetch_summary(spark, args.hive_db, args.export_year_month)
        if len(rows) == 0:
            raise RuntimeError(
                f"No rows found in {args.hive_db}.shipment_summary "
                f"for export_year_month={args.export_year_month}"
            )
        write_csv(args.output_path, EXPORT_COLUMNS, rows)

        write_audit_log(
            spark=spark,
            hive_db=args.hive_db,
            job_name=args.job_name,
            run_dt=run_dt_str,
            export_month=args.export_year_month,
            export_file=args.output_path,
            record_cnt=len(rows),
            status="SUCCESS",
            error_message="",
        )
        print(
            f"Export OK month={args.export_year_month} "
            f"rows={len(rows)} file={args.output_path}"
        )
    except Exception as exc:  # pragma: no cover - exercised via integration runs
        err_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        try:
            write_audit_log(
                spark=spark,
                hive_db=args.hive_db,
                job_name=args.job_name,
                run_dt=run_dt_str,
                export_month=args.export_year_month,
                export_file=args.output_path,
                record_cnt=0,
                status="FAILED",
                error_message=err_msg[:4000],
            )
        finally:
            print(f"Export FAILED: {err_msg}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

    sys.exit(0)


if __name__ == "__main__":
    main()
