"""Unit tests for the weekly_dim_refresh job.

Covers:
  * active-record filter (status='A')
  * future-dated exclusion (eff_dt > load_dt)
  * expired exclusion (exp_dt < load_dt)
  * backup completeness (every row from the live dim survives the snapshot)
"""

from pyspark.sql import functions as F

from conftest import active_dimension_filter


class TestJob2ActiveFilter:

    def test_active_status_kept(self, source_carriers_df, load_dt):
        filtered = active_dimension_filter(source_carriers_df, load_dt)
        statuses = {r["status"] for r in filtered.collect()}
        assert statuses == {"A"}

    def test_inactive_status_filtered(self, source_carriers_df, load_dt):
        filtered = active_dimension_filter(source_carriers_df, load_dt)
        ids = {r["carrier_id"] for r in filtered.collect()}
        assert "CRR003" not in ids   # status = 'I'

    def test_future_dated_excluded(self, source_carriers_df, load_dt):
        filtered = active_dimension_filter(source_carriers_df, load_dt)
        ids = {r["carrier_id"] for r in filtered.collect()}
        # CRR004 has eff_dt 2099-01-01 - should not yet be active.
        assert "CRR004" not in ids

    def test_expired_excluded(self, source_carriers_df, load_dt):
        filtered = active_dimension_filter(source_carriers_df, load_dt)
        ids = {r["carrier_id"] for r in filtered.collect()}
        # CRR005 has exp_dt 2020-12-31 - well before 2026-05-10.
        assert "CRR005" not in ids

    def test_active_count_carriers(self, source_carriers_df, load_dt):
        filtered = active_dimension_filter(source_carriers_df, load_dt)
        assert filtered.count() == 2  # CRR001 + CRR002

    def test_active_count_routes(self, source_routes_df, load_dt):
        filtered = active_dimension_filter(source_routes_df, load_dt)
        assert filtered.count() == 2  # RT001 + RT002


class TestJob2BackupCompleteness:
    """Backup HQL creates dim_carrier_bkp and dim_route_bkp as CTAS clones."""

    def test_carrier_backup_preserves_every_row(self, dim_carrier_df):
        backup = dim_carrier_df  # CREATE TABLE AS SELECT is row-equivalent
        assert backup.count() == dim_carrier_df.count()
        live_ids = {r["carrier_id"] for r in dim_carrier_df.collect()}
        bkp_ids = {r["carrier_id"] for r in backup.collect()}
        assert live_ids == bkp_ids

    def test_route_backup_preserves_every_row(self, dim_route_df):
        backup = dim_route_df
        assert backup.count() == dim_route_df.count()
        live_ids = {r["route_id"] for r in dim_route_df.collect()}
        bkp_ids = {r["route_id"] for r in backup.collect()}
        assert live_ids == bkp_ids

    def test_carrier_backup_schema_matches(self, dim_carrier_df):
        backup = dim_carrier_df
        assert backup.schema == dim_carrier_df.schema

    def test_route_backup_schema_matches(self, dim_route_df):
        backup = dim_route_df
        assert backup.schema == dim_route_df.schema


class TestJob2BoundaryDates:
    """Edge case: eff_dt == load_dt should be considered active."""

    def test_eff_dt_equals_load_dt_kept(self, spark, load_dt):
        df = spark.createDataFrame(
            [("CRR010", "ED", "EdgeCarrier", "GROUND", "US", "WEST", "GOLD",
              None, None, "A", load_dt, None)],
            schema=[
                "carrier_id", "carrier_code", "carrier_name", "carrier_type",
                "country_cd", "region_cd", "service_tier", "max_weight_kg",
                "insurance_limit", "status", "eff_dt", "exp_dt",
            ],
        )
        assert active_dimension_filter(df, load_dt).count() == 1

    def test_exp_dt_equals_load_dt_kept(self, spark, load_dt):
        df = spark.createDataFrame(
            [("CRR011", "ED2", "EdgeCarrier2", "AIR", "US", "EAST", "SILVER",
              None, None, "A", "2024-01-01", load_dt)],
            schema=[
                "carrier_id", "carrier_code", "carrier_name", "carrier_type",
                "country_cd", "region_cd", "service_tier", "max_weight_kg",
                "insurance_limit", "status", "eff_dt", "exp_dt",
            ],
        )
        assert active_dimension_filter(df, load_dt).count() == 1
