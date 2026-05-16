"""Shared pytest fixtures for the shiplogistics unit-test suite.

These fixtures build small in-memory PySpark DataFrames that mirror the
shape of the on-prem Hive tables. The sample rows are deliberately
constructed so that filter, dedup, join and aggregation logic can be
exercised without ever reading a file or HQL script.
"""

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------
#
# SAMPLE_SOURCE_SHIPMENTS columns (in order):
#   shipment_id, tracking_number, origin_facility_cd, destination_facility_cd,
#   carrier_id, route_id, record_type, shipment_status_code, service_level,
#   booking_channel, customer_id, weight_kg, volume_cbm, declared_value,
#   shipping_charge, pickup_dt, expected_delivery_dt, delivery_dt, trans_dt
#
# Rows are tagged in comments with their expected fate under the daily ingest
# filters (filter_excl_test_type, filter_excl_internal_type,
# filter_status_code, filter_min_value, filter_non_null_carrier).
# ---------------------------------------------------------------------------
SAMPLE_SOURCE_SHIPMENTS = [
    # VALID: passes every filter, joins to carrier and route, no DQ error.
    ("SHIP00001", "TRK00001", "NYC01", "LAX02", "CRR001", "RT001",
     "RAW", "DELIVERED", "EXPRESS", "WEB", "CUST001",
     Decimal("12.50"), Decimal("0.0500"), Decimal("250.00"), Decimal("15.00"),
     "2026-05-13", "2026-05-15", "2026-05-15", "2026-05-14"),
    # FILTERED: record_type='TEST'
    ("SHIP00002", "TRK00002", "NYC01", "ORD03", "CRR001", "RT002",
     "TEST", "DELIVERED", "STANDARD", "WEB", "CUST002",
     Decimal("5.00"), Decimal("0.0100"), Decimal("100.00"), Decimal("8.00"),
     "2026-05-13", "2026-05-15", "2026-05-15", "2026-05-14"),
    # FILTERED: record_type='INTERNAL'
    ("SHIP00003", "TRK00003", "NYC01", "ORD03", "CRR002", "RT001",
     "INTERNAL", "BOOKED", "STANDARD", "WEB", "CUST003",
     Decimal("50.00"), Decimal("0.2500"), Decimal("500.00"), Decimal("30.00"),
     "2026-05-13", "2026-05-16", "", "2026-05-14"),
    # FILTERED: status CANCELLED
    ("SHIP00004", "TRK00004", "NYC01", "LAX02", "CRR001", "RT001",
     "RAW", "CANCELLED", "STANDARD", "WEB", "CUST004",
     Decimal("8.00"), Decimal("0.0300"), Decimal("75.00"), Decimal("10.00"),
     "2026-05-13", "2026-05-15", "", "2026-05-14"),
    # FILTERED: below min_declared_value (10.00)
    ("SHIP00005", "TRK00005", "NYC01", "LAX02", "CRR002", "RT002",
     "RAW", "DELIVERED", "STANDARD", "WEB", "CUST005",
     Decimal("3.00"), Decimal("0.0100"), Decimal("5.50"), Decimal("4.00"),
     "2026-05-13", "2026-05-15", "2026-05-14", "2026-05-14"),
    # VALID: passes filters
    ("SHIP00006", "TRK00006", "NYC01", "DFW04", "CRR002", "RT002",
     "RAW", "IN_TRANSIT", "EXPRESS", "AGENT", "CUST006",
     Decimal("20.00"), Decimal("0.0800"), Decimal("400.00"), Decimal("25.00"),
     "2026-05-13", "2026-05-16", "", "2026-05-14"),
    # DQ ERROR: negative declared_value
    ("SHIP00007", "TRK00007", "NYC01", "LAX02", "CRR001", "RT001",
     "RAW", "DELIVERED", "STANDARD", "WEB", "CUST007",
     Decimal("15.00"), Decimal("0.0500"), Decimal("-25.00"), Decimal("12.00"),
     "2026-05-13", "2026-05-15", "2026-05-15", "2026-05-14"),
    # DQ ERROR: NULL mandatory field tracking_number
    ("SHIP00008", None, "NYC01", "LAX02", "CRR002", "RT001",
     "RAW", "BOOKED", "STANDARD", "WEB", "CUST008",
     Decimal("10.00"), Decimal("0.0500"), Decimal("100.00"), Decimal("12.00"),
     "2026-05-13", "2026-05-16", "", "2026-05-14"),
    # DQ ERROR: carrier_id 'CRR999' not in dim_carrier
    ("SHIP00009", "TRK00009", "NYC01", "LAX02", "CRR999", "RT001",
     "RAW", "DELIVERED", "STANDARD", "WEB", "CUST009",
     Decimal("11.00"), Decimal("0.0500"), Decimal("150.00"), Decimal("12.00"),
     "2026-05-13", "2026-05-15", "2026-05-15", "2026-05-14"),
    # DUPLICATE of SHIP00001 - exact duplicate row used by the dedup test
    ("SHIP00001", "TRK00001", "NYC01", "LAX02", "CRR001", "RT001",
     "RAW", "DELIVERED", "EXPRESS", "WEB", "CUST001",
     Decimal("12.50"), Decimal("0.0500"), Decimal("250.00"), Decimal("15.00"),
     "2026-05-13", "2026-05-15", "2026-05-15", "2026-05-14"),
]

SAMPLE_SHIPMENTS_COLUMNS = [
    "shipment_id", "tracking_number", "origin_facility_cd",
    "destination_facility_cd", "carrier_id", "route_id", "record_type",
    "shipment_status_code", "service_level", "booking_channel",
    "customer_id", "weight_kg", "volume_cbm", "declared_value",
    "shipping_charge", "pickup_dt", "expected_delivery_dt", "delivery_dt",
    "trans_dt",
]


# ---------------------------------------------------------------------------
# Carrier master sample. status='A' eff_dt<=load_dt exp_dt IS NULL/>=load_dt
# ---------------------------------------------------------------------------
SAMPLE_SOURCE_CARRIERS = [
    # ACTIVE
    ("CRR001", "FX", "FleetExpress",   "GROUND", "US", "WEST",   "GOLD",
     Decimal("1000.00"), Decimal("50000.00"), "A", "2025-01-01", None),
    # ACTIVE (exp_dt empty string)
    ("CRR002", "BX", "BlueExpress",    "AIR",    "US", "EAST",   "SILVER",
     Decimal("500.00"),  Decimal("25000.00"), "A", "2024-06-01", ""),
    # INACTIVE - status != 'A'
    ("CRR003", "RT", "RedTransport",   "GROUND", "US", "SOUTH",  "BRONZE",
     Decimal("750.00"),  Decimal("10000.00"), "I", "2020-01-01", "2025-12-31"),
    # FUTURE-DATED - eff_dt > load_dt
    ("CRR004", "NL", "NewLine",        "AIR",    "US", "WEST",   "GOLD",
     Decimal("800.00"),  Decimal("40000.00"), "A", "2099-01-01", None),
    # EXPIRED - exp_dt < load_dt
    ("CRR005", "OL", "OldLine",        "GROUND", "US", "EAST",   "BRONZE",
     Decimal("600.00"),  Decimal("20000.00"), "A", "2018-01-01", "2020-12-31"),
]

SAMPLE_CARRIERS_COLUMNS = [
    "carrier_id", "carrier_code", "carrier_name", "carrier_type",
    "country_cd", "region_cd", "service_tier", "max_weight_kg",
    "insurance_limit", "status", "eff_dt", "exp_dt",
]


# ---------------------------------------------------------------------------
# Route master sample - mirrors the carrier master active/expired/future mix
# ---------------------------------------------------------------------------
SAMPLE_SOURCE_ROUTES = [
    ("RT001", "NYC-LAX", "NYC to LAX express",  "NYC01", "LAX02",
     "AIR",    Decimal("3940.00"), 2, Decimal("1500.00"),
     Decimal("12.50"), "WEST",   "A", "2024-01-01", None),
    ("RT002", "NYC-DFW", "NYC to DFW ground",   "NYC01", "DFW04",
     "GROUND", Decimal("2200.00"), 4, Decimal("800.00"),
     Decimal("10.00"), "SOUTH",  "A", "2024-01-01", ""),
    ("RT003", "NYC-ORD", "NYC to Chicago",      "NYC01", "ORD03",
     "GROUND", Decimal("1300.00"), 3, Decimal("500.00"),
     Decimal("9.50"),  "EAST",   "I", "2022-01-01", "2025-06-30"),
    ("RT004", "NYC-SEA", "Future Seattle line", "NYC01", "SEA05",
     "AIR",    Decimal("3870.00"), 2, Decimal("1400.00"),
     Decimal("11.00"), "WEST",   "A", "2099-01-01", None),
    ("RT005", "NYC-MIA", "Retired Miami line",  "NYC01", "MIA06",
     "GROUND", Decimal("2050.00"), 4, Decimal("700.00"),
     Decimal("10.50"), "SOUTH",  "A", "2019-01-01", "2021-12-31"),
]

SAMPLE_ROUTES_COLUMNS = [
    "route_id", "route_code", "route_name", "origin_facility_cd",
    "destination_facility_cd", "transit_mode", "distance_km",
    "standard_transit_days", "base_cost", "fuel_surcharge_pct", "region_cd",
    "status", "eff_dt", "exp_dt",
]


# ---------------------------------------------------------------------------
# Dimension samples (post-refresh shape - active rows only)
# ---------------------------------------------------------------------------
SAMPLE_DIM_CARRIER = [
    ("CRR001", "FX", "FleetExpress", "GROUND", "US", "WEST",
     "GOLD",   Decimal("1000.00"), Decimal("50000.00"),
     "A", "2025-01-01", None, "2026-05-10 02:00:00"),
    ("CRR002", "BX", "BlueExpress",  "AIR",    "US", "EAST",
     "SILVER", Decimal("500.00"),  Decimal("25000.00"),
     "A", "2024-06-01", None, "2026-05-10 02:00:00"),
]

SAMPLE_DIM_CARRIER_COLUMNS = [
    "carrier_id", "carrier_code", "carrier_name", "carrier_type",
    "country_cd", "region_cd", "service_tier", "max_weight_kg",
    "insurance_limit", "status", "eff_dt", "exp_dt", "load_timestamp",
]


SAMPLE_DIM_ROUTE = [
    ("RT001", "NYC-LAX", "NYC to LAX express", "NYC01", "LAX02",
     "AIR",    Decimal("3940.00"), 2, Decimal("1500.00"),
     Decimal("12.50"), "WEST",  "A", "2024-01-01", None,
     "2026-05-10 02:00:00"),
    ("RT002", "NYC-DFW", "NYC to DFW ground",  "NYC01", "DFW04",
     "GROUND", Decimal("2200.00"), 4, Decimal("800.00"),
     Decimal("10.00"), "SOUTH", "A", "2024-01-01", None,
     "2026-05-10 02:00:00"),
]

SAMPLE_DIM_ROUTE_COLUMNS = [
    "route_id", "route_code", "route_name", "origin_facility_cd",
    "destination_facility_cd", "transit_mode", "distance_km",
    "standard_transit_days", "base_cost", "fuel_surcharge_pct", "region_cd",
    "status", "eff_dt", "exp_dt", "load_timestamp",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("shiplogistics-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def source_shipments_df(spark):
    return spark.createDataFrame(
        SAMPLE_SOURCE_SHIPMENTS, schema=SAMPLE_SHIPMENTS_COLUMNS
    )


@pytest.fixture(scope="session")
def source_carriers_df(spark):
    return spark.createDataFrame(
        SAMPLE_SOURCE_CARRIERS, schema=SAMPLE_CARRIERS_COLUMNS
    )


@pytest.fixture(scope="session")
def source_routes_df(spark):
    return spark.createDataFrame(
        SAMPLE_SOURCE_ROUTES, schema=SAMPLE_ROUTES_COLUMNS
    )


@pytest.fixture(scope="session")
def dim_carrier_df(spark):
    return spark.createDataFrame(
        SAMPLE_DIM_CARRIER, schema=SAMPLE_DIM_CARRIER_COLUMNS
    )


@pytest.fixture(scope="session")
def dim_route_df(spark):
    return spark.createDataFrame(
        SAMPLE_DIM_ROUTE, schema=SAMPLE_DIM_ROUTE_COLUMNS
    )


@pytest.fixture(scope="session")
def load_dt():
    """The load_dt used by the weekly_dim_refresh tests."""
    return "2026-05-10"


@pytest.fixture(scope="session")
def trans_dt():
    """The trans_dt used by all daily-job tests."""
    return "2026-05-14"


# ---------------------------------------------------------------------------
# Re-usable transform helpers (mirror the production HQL semantics)
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F  # noqa: E402  intentional below fixtures


def apply_ingest_filters(df, min_declared_value=Decimal("10.00")):
    """Mirror the filter set defined in daily_shipment_ingest_filters.config."""
    return df.filter(
        (F.col("record_type") != "TEST")
        & (F.col("record_type") != "INTERNAL")
        & (F.col("shipment_status_code").isin("BOOKED", "IN_TRANSIT", "DELIVERED"))
        & (F.col("declared_value") >= F.lit(min_declared_value))
        & (F.col("carrier_id").isNotNull())
        & (F.col("carrier_id") != "")
    )


def active_dimension_filter(df, load_dt):
    """Mirror weekly_dim_refresh_filters.config (status='A' / eff_dt / exp_dt)."""
    return df.filter(
        (F.col("status") == "A")
        & (F.col("eff_dt") <= load_dt)
        & (F.col("exp_dt").isNull() | (F.col("exp_dt") == "") | (F.col("exp_dt") >= load_dt))
    )
