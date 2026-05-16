"""Unit tests for the daily_shipment_summary job.

Covers:
  * aggregation counts (total / approved / rejected)
  * error rows correctly identified (ERR_NO_DIM_CARRIER, ERR_NEG_VALUE,
    ERR_NULL_MANDATORY)
  * approval rate math (percentage string)
  * integer error-rate threshold (below and above warn level)
"""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import apply_ingest_filters


# ---------------------------------------------------------------------------
# Pure-Python helpers that mirror the HQL semantics
# ---------------------------------------------------------------------------

def approval_rate_str(approved, total):
    """Mirror the CONCAT(CAST(ROUND(...,2) AS STRING), '%') expression."""
    if total == 0:
        return "0.00%"
    pct = round(approved * 100.0 / total, 2)
    return f"{pct:.2f}%"


def integer_error_rate(err_cnt, stg_cnt):
    """Mirror the shell script integer-division err_cnt * 100 / stg_cnt."""
    if stg_cnt == 0:
        return 0
    return (err_cnt * 100) // stg_cnt


# ---------------------------------------------------------------------------
# Fixtures derived from the canonical source_shipments fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def stg_df(source_shipments_df):
    """The post-filter staging table that Job 3 reads from."""
    return apply_ingest_filters(source_shipments_df)


@pytest.fixture(scope="module")
def summary_df(stg_df, dim_carrier_df, dim_route_df):
    """Recreate the INNER JOIN + aggregation that daily_shipment_summary_load.hql produces."""
    joined = (
        stg_df.alias("s")
        .join(dim_carrier_df.alias("dc"), F.col("s.carrier_id") == F.col("dc.carrier_id"), "inner")
        .join(dim_route_df.alias("dr"),   F.col("s.route_id")   == F.col("dr.route_id"),   "inner")
    )
    agg = joined.groupBy(
        F.col("s.trans_dt").alias("report_date"),
        F.col("s.carrier_id"),
        F.col("dc.carrier_name"),
        F.col("s.route_id"),
        F.col("dr.region_cd"),
        F.col("dr.transit_mode"),
        F.col("s.service_level"),
    ).agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("s.shipment_status_code") == "DELIVERED", 1).otherwise(0)).alias("approved_count"),
        F.sum(F.when(F.col("s.shipment_status_code").isin("CANCELLED", "RETURNED"), 1).otherwise(0)).alias("rejected_count"),
        F.sum(F.coalesce(F.col("s.declared_value"), F.lit(Decimal("0")))).alias("total_amount"),
        F.sum(F.coalesce(F.col("s.weight_kg"),     F.lit(Decimal("0")))).alias("total_weight_kg"),
    )
    return agg


@pytest.fixture(scope="module")
def dq_df(stg_df, dim_carrier_df, dim_route_df):
    """Recreate the DQ UNION ALL output of daily_shipment_summary_dq.hql."""

    # ERR_NO_DIM_CARRIER
    no_carrier = (
        stg_df.alias("s")
        .join(dim_carrier_df.alias("dc"),
              F.col("s.carrier_id") == F.col("dc.carrier_id"), "left_outer")
        .filter(F.col("dc.carrier_id").isNull())
        .select(F.col("s.shipment_id"), F.lit("ERR_NO_DIM_CARRIER").alias("error_code"))
    )

    # ERR_NO_DIM_ROUTE
    no_route = (
        stg_df.alias("s")
        .join(dim_route_df.alias("dr"),
              F.col("s.route_id") == F.col("dr.route_id"), "left_outer")
        .filter(F.col("dr.route_id").isNull())
        .select(F.col("s.shipment_id"), F.lit("ERR_NO_DIM_ROUTE").alias("error_code"))
    )

    # ERR_NEG_VALUE
    neg_value = (
        stg_df.filter(
            (F.col("declared_value") < F.lit(Decimal("0")))
            | (F.col("shipping_charge") < F.lit(Decimal("0")))
        ).select(F.col("shipment_id"), F.lit("ERR_NEG_VALUE").alias("error_code"))
    )

    # ERR_NULL_MANDATORY
    null_mand = (
        stg_df.filter(
            F.col("tracking_number").isNull() | (F.col("tracking_number") == "")
            | F.col("origin_facility_cd").isNull() | (F.col("origin_facility_cd") == "")
            | F.col("destination_facility_cd").isNull() | (F.col("destination_facility_cd") == "")
            | F.col("weight_kg").isNull()
        ).select(F.col("shipment_id"), F.lit("ERR_NULL_MANDATORY").alias("error_code"))
    )

    return no_carrier.unionByName(no_route).unionByName(neg_value).unionByName(null_mand)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestJob3AggregationCounts:

    def test_total_count_only_includes_joinable_rows(self, summary_df):
        # SHIP00009's carrier_id=CRR999 is NOT in dim_carrier so it does
        # NOT contribute to the aggregate.
        total = summary_df.agg(F.sum("total_count")).collect()[0][0]
        # SHIP00001 (x2 dup) + SHIP00006 + SHIP00007 + SHIP00008 = 5 rows survive
        # the filter AND join (SHIP00009 fails join, gets logged as DQ).
        assert total == 5

    def test_approved_count_matches_delivered(self, summary_df):
        approved = summary_df.agg(F.sum("approved_count")).collect()[0][0]
        # Delivered rows that join: SHIP00001 (x2), SHIP00007.
        assert approved == 3

    def test_rejected_count_zero_after_status_filter(self, summary_df):
        rejected = summary_df.agg(F.sum("rejected_count")).collect()[0][0]
        # The status filter already drops CANCELLED + RETURNED.
        assert rejected == 0


class TestJob3DqErrorIdentification:

    def test_neg_value_flagged(self, dq_df):
        rows = dq_df.filter(F.col("error_code") == "ERR_NEG_VALUE").collect()
        flagged = {r["shipment_id"] for r in rows}
        assert "SHIP00007" in flagged

    def test_null_mandatory_flagged(self, dq_df):
        rows = dq_df.filter(F.col("error_code") == "ERR_NULL_MANDATORY").collect()
        flagged = {r["shipment_id"] for r in rows}
        assert "SHIP00008" in flagged

    def test_unknown_carrier_flagged(self, dq_df):
        rows = dq_df.filter(F.col("error_code") == "ERR_NO_DIM_CARRIER").collect()
        flagged = {r["shipment_id"] for r in rows}
        assert "SHIP00009" in flagged

    def test_clean_records_not_flagged(self, dq_df):
        flagged = {r["shipment_id"] for r in dq_df.collect()}
        assert "SHIP00001" not in flagged
        assert "SHIP00006" not in flagged


class TestJob3ApprovalRateMath:

    def test_50_percent(self):
        assert approval_rate_str(approved=1, total=2) == "50.00%"

    def test_two_thirds(self):
        assert approval_rate_str(approved=2, total=3) == "66.67%"

    def test_zero_total_safe(self):
        assert approval_rate_str(approved=0, total=0) == "0.00%"

    def test_all_approved(self):
        assert approval_rate_str(approved=10, total=10) == "100.00%"


class TestJob3IntegerErrorRateThreshold:
    """Mirrors `err_rate=$(( err_cnt * 100 / stg_cnt ))` in trigger.sh."""

    def test_below_threshold_no_warn(self):
        # 2 errors over 100 staging rows -> 2% -> below 5%
        rate = integer_error_rate(err_cnt=2, stg_cnt=100)
        assert rate == 2
        assert rate <= 5

    def test_above_threshold_warn(self):
        # 8 errors over 100 staging rows -> 8% -> above 5%
        rate = integer_error_rate(err_cnt=8, stg_cnt=100)
        assert rate == 8
        assert rate > 5

    def test_exact_threshold_not_warned(self):
        # Trigger uses strict `>` so exactly at the threshold should NOT warn.
        rate = integer_error_rate(err_cnt=5, stg_cnt=100)
        assert rate == 5
        # strict greater-than: 5 > 5 is False
        assert not (rate > 5)

    def test_integer_truncation_matches_shell(self):
        # 4 / 99 in integer math: 4 * 100 // 99 = 4
        assert integer_error_rate(err_cnt=4, stg_cnt=99) == 4

    def test_zero_staging_returns_zero(self):
        # Guard against div-by-zero in test helper. Production HQL ensures
        # stg_cnt > 0 before this code path is reached.
        assert integer_error_rate(err_cnt=3, stg_cnt=0) == 0
