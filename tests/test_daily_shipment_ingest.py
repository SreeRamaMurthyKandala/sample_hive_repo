"""Unit tests for the daily_shipment_ingest job.

These tests mirror the filter and dedup logic in the production HQL files
without reading them. Required coverage:

  * filter excludes expected rows
  * dedup detection (clean vs. polluted input)
  * post-filter count is exactly the set of valid + DQ rows that pass the
    filter set (the DQ rows still survive the filters, they are caught later
    by Job 3's DQ HQL)
"""

from decimal import Decimal

from pyspark.sql import functions as F

from conftest import apply_ingest_filters


class TestJob1FilterLogic:

    def test_excl_test_type_filtered(self, source_shipments_df):
        filtered = apply_ingest_filters(source_shipments_df)
        types = {r["record_type"] for r in filtered.collect()}
        assert "TEST" not in types

    def test_excl_internal_type_filtered(self, source_shipments_df):
        filtered = apply_ingest_filters(source_shipments_df)
        types = {r["record_type"] for r in filtered.collect()}
        assert "INTERNAL" not in types

    def test_cancelled_status_filtered(self, source_shipments_df):
        filtered = apply_ingest_filters(source_shipments_df)
        statuses = {r["shipment_status_code"] for r in filtered.collect()}
        assert "CANCELLED" not in statuses
        assert "RETURNED" not in statuses

    def test_below_threshold_filtered(self, source_shipments_df):
        filtered = apply_ingest_filters(source_shipments_df, Decimal("10.00"))
        ids = {r["shipment_id"] for r in filtered.collect()}
        # SHIP00005 has declared_value 5.50 - below the 10.00 threshold
        assert "SHIP00005" not in ids

    def test_valid_records_pass(self, source_shipments_df):
        filtered = apply_ingest_filters(source_shipments_df)
        ids = {r["shipment_id"] for r in filtered.collect()}
        # SHIP00001 and SHIP00006 are the canonical "all good" rows
        assert "SHIP00001" in ids
        assert "SHIP00006" in ids

    def test_post_filter_count(self, source_shipments_df):
        filtered = apply_ingest_filters(source_shipments_df)
        # Expected survivors (from conftest commentary):
        #   SHIP00001 (valid), SHIP00006 (valid),
        #   SHIP00007 (DQ neg_value, but passes pre-filters),
        #   SHIP00008 (DQ null mandatory, but passes pre-filters),
        #   SHIP00009 (DQ unknown FK, but passes pre-filters),
        #   plus duplicate SHIP00001 row (= 6 total).
        # SHIP00002 TEST, SHIP00003 INTERNAL, SHIP00004 CANCELLED,
        # SHIP00005 below_threshold should NOT pass.
        assert filtered.count() == 6


class TestJob1DedupCheck:
    """Mirrors daily_shipment_ingest_check.hql semantics."""

    @staticmethod
    def _dup_count(df):
        grouped = df.groupBy("shipment_id", "trans_dt").agg(F.count("*").alias("cnt"))
        return grouped.filter(F.col("cnt") > 1).count()

    def test_no_dups_in_clean_data(self, source_shipments_df):
        clean = source_shipments_df.dropDuplicates(["shipment_id", "trans_dt"])
        assert self._dup_count(clean) == 0

    def test_detects_added_duplicate(self, source_shipments_df):
        # The fixture intentionally includes a duplicate SHIP00001 row.
        assert self._dup_count(source_shipments_df) == 1

    def test_detects_multiple_distinct_duplicates(self, source_shipments_df, spark):
        # Inject a second duplicate pair (SHIP00006) and confirm two pairs flagged.
        extra_row = source_shipments_df.filter(
            F.col("shipment_id") == "SHIP00006"
        )
        polluted = source_shipments_df.unionByName(extra_row)
        assert self._dup_count(polluted) == 2
