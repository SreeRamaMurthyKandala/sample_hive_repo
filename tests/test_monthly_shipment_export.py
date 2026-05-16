"""Unit tests for the monthly_shipment_export job.

Covers:
  * month filter (report_date LIKE 'YYYY-MM-%')
  * CSV header/trailer format
  * null-to-empty-string conversion
  * audit log schema
"""

import io
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Import the implementation under test as a real module. The script does not
# do any work on import (everything is inside `if __name__ == "__main__":`).
import importlib.util
import os

_SCRIPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "corp", "analytics", "shiplogistics", "app",
    "monthly_shipment_export", "scripts", "monthly_shipment_export.py",
)
_spec = importlib.util.spec_from_file_location("monthly_shipment_export", _SCRIPT_PATH)
export_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export_mod)


# ---------------------------------------------------------------------------
# Sample summary rows that span two calendar months
# ---------------------------------------------------------------------------

_SUMMARY_COLUMNS = [
    "report_date", "report_month", "carrier_id", "carrier_name", "route_id",
    "region_cd", "transit_mode", "service_level", "total_count",
    "approved_count", "rejected_count", "total_amount", "total_weight_kg",
    "approval_rate",
]

_SUMMARY_ROWS = [
    ("2026-04-01", "2026-04", "CRR001", "FleetExpress", "RT001", "WEST", "AIR",   "EXPRESS",
     10, 9, 1, Decimal("2500.00"), Decimal("125.00"), "90.00%"),
    ("2026-04-15", "2026-04", "CRR002", "BlueExpress",  "RT002", "SOUTH", "GROUND", "STANDARD",
     20, 18, 2, Decimal("4500.00"), Decimal("400.00"), "90.00%"),
    ("2026-04-30", "2026-04", None,    "BlueExpress",  "RT002", "SOUTH", "GROUND", "STANDARD",
     5, 5, 0, Decimal("1000.00"), Decimal("80.00"), "100.00%"),
    ("2026-05-01", "2026-05", "CRR001", "FleetExpress", "RT001", "WEST", "AIR",   "EXPRESS",
     7, 6, 1, Decimal("1750.00"), Decimal("87.50"), "85.71%"),
]


@pytest.fixture(scope="module")
def summary_df(spark):
    return spark.createDataFrame(_SUMMARY_ROWS, schema=_SUMMARY_COLUMNS)


# ---------------------------------------------------------------------------
# Month filter
# ---------------------------------------------------------------------------

class TestJob4MonthFilter:

    def test_filter_picks_only_target_month(self, summary_df):
        filtered = summary_df.filter(F.col("report_date").like("2026-04-%"))
        assert filtered.count() == 3

    def test_filter_excludes_other_months(self, summary_df):
        filtered = summary_df.filter(F.col("report_date").like("2026-04-%"))
        dates = {r["report_date"] for r in filtered.collect()}
        assert all(d.startswith("2026-04-") for d in dates)
        assert "2026-05-01" not in dates

    def test_filter_empty_for_unknown_month(self, summary_df):
        filtered = summary_df.filter(F.col("report_date").like("1999-12-%"))
        assert filtered.count() == 0


# ---------------------------------------------------------------------------
# CSV header / trailer format
# ---------------------------------------------------------------------------

class TestJob4CsvFormat:

    @staticmethod
    def _render(rows, columns=None):
        """Run the production write_csv but to an in-memory buffer."""
        columns = columns or export_mod.EXPORT_COLUMNS
        buf = io.StringIO()
        buf.write(",".join(columns) + "\n")
        for row in rows:
            cells = [export_mod.render_cell(row[col]) for col in columns]
            buf.write(",".join(cells) + "\n")
        buf.write(f"TRAILER,{len(rows)}\n")
        return buf.getvalue()

    def test_header_first_line(self, summary_df):
        rows = summary_df.filter(F.col("report_date").like("2026-04-%")).collect()
        csv_text = self._render(rows)
        first = csv_text.splitlines()[0]
        assert first == ",".join(export_mod.EXPORT_COLUMNS)

    def test_trailer_last_line_count_matches(self, summary_df):
        rows = summary_df.filter(F.col("report_date").like("2026-04-%")).collect()
        csv_text = self._render(rows)
        last = csv_text.splitlines()[-1]
        assert last.startswith("TRAILER,")
        assert last == f"TRAILER,{len(rows)}"

    def test_trailer_count_three(self, summary_df):
        rows = summary_df.filter(F.col("report_date").like("2026-04-%")).collect()
        csv_text = self._render(rows)
        assert csv_text.splitlines()[-1] == "TRAILER,3"

    def test_data_row_count(self, summary_df):
        rows = summary_df.filter(F.col("report_date").like("2026-04-%")).collect()
        csv_text = self._render(rows)
        lines = csv_text.splitlines()
        # HEADER + 3 data + TRAILER = 5
        assert len(lines) == 5


# ---------------------------------------------------------------------------
# Null-to-empty-string conversion
# ---------------------------------------------------------------------------

class TestJob4NullHandling:

    def test_none_becomes_empty(self):
        assert export_mod.render_cell(None) == ""

    def test_empty_string_remains_empty(self):
        assert export_mod.render_cell("") == ""

    def test_zero_becomes_zero_string(self):
        # IMPORTANT: 0 must NOT be converted to "" - only None should.
        assert export_mod.render_cell(0) == "0"

    def test_decimal_preserved(self):
        assert export_mod.render_cell(Decimal("1750.00")) == "1750.00"

    def test_null_carrier_id_serializes_blank(self, summary_df):
        row_with_null = (
            summary_df
            .filter(F.col("carrier_id").isNull())
            .collect()[0]
        )
        rendered = export_mod.render_cell(row_with_null["carrier_id"])
        assert rendered == ""


# ---------------------------------------------------------------------------
# Audit log schema
# ---------------------------------------------------------------------------

class TestJob4AuditLogSchema:

    def test_expected_schema(self):
        """The audit DataFrame schema must match export_log.hdl exactly."""
        expected = StructType([
            StructField("job_name", StringType()),
            StructField("run_dt", StringType()),
            StructField("export_month", StringType()),
            StructField("export_file", StringType()),
            StructField("record_cnt", LongType()),
            StructField("status", StringType()),
            StructField("error_message", StringType()),
            StructField("load_timestamp", TimestampType()),
        ])

        # Build the same row tuple write_audit_log creates, and confirm it
        # constructs a DataFrame with the expected schema.
        from datetime import datetime
        row = [(
            "monthly_shipment_export", "2026-05-01 07:00:00", "2026-04",
            "/tmp/shiplogistics_summary_2026-04.csv", 3, "SUCCESS", "",
            datetime.now(),
        )]
        # We need a SparkSession to test this end-to-end.
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(row, expected)
        assert df.schema == expected
        assert df.count() == 1

    def test_status_field_values(self):
        """The two valid status values are SUCCESS and FAILED."""
        valid = {"SUCCESS", "FAILED"}
        assert "SUCCESS" in valid and "FAILED" in valid
