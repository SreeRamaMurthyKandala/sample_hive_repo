-- ----------------------------------------------------------------------------
-- weekly_dim_refresh_backup.hql
--
-- Snapshot the current dim tables into _bkp tables before the full refresh
-- runs. Allows manual rollback in case of catastrophic upstream corruption.
--
-- Variables expected from -d on hive-runner:
--   dim_db, mapred_qname
-- ----------------------------------------------------------------------------

USE ${dim_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;

-- -------------------- dim_carrier_bkp -------------------- --
DROP TABLE IF EXISTS ${dim_db}.dim_carrier_bkp;

CREATE TABLE ${dim_db}.dim_carrier_bkp
STORED AS TEXTFILE
AS
SELECT
  carrier_id,
  carrier_code,
  carrier_name,
  carrier_type,
  country_cd,
  region_cd,
  service_tier,
  max_weight_kg,
  insurance_limit,
  status,
  eff_dt,
  exp_dt,
  load_timestamp
FROM ${dim_db}.dim_carrier;

-- -------------------- dim_route_bkp -------------------- --
DROP TABLE IF EXISTS ${dim_db}.dim_route_bkp;

CREATE TABLE ${dim_db}.dim_route_bkp
STORED AS TEXTFILE
AS
SELECT
  route_id,
  route_code,
  route_name,
  origin_facility_cd,
  destination_facility_cd,
  transit_mode,
  distance_km,
  standard_transit_days,
  base_cost,
  fuel_surcharge_pct,
  region_cd,
  status,
  eff_dt,
  exp_dt,
  load_timestamp
FROM ${dim_db}.dim_route;
