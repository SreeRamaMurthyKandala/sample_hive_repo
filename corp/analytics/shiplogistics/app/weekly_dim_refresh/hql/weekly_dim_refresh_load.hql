-- ----------------------------------------------------------------------------
-- weekly_dim_refresh_load.hql
--
-- Full-refresh both dim_carrier and dim_route, keeping only active rows as
-- of ${load_dt}. The filter set is injected via -i and shared across both
-- INSERT OVERWRITE statements.
--
-- Variables expected from -d on hive-runner:
--   cs_db, dim_db, mapred_qname, load_dt
-- Variables expected from -i (filters file):
--   filter_active_status, filter_eff_dt, filter_exp_dt
-- ----------------------------------------------------------------------------

USE ${dim_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts=-Xmx3072m;
SET hive.exec.compress.output=false;

-- -------------------- Carrier dimension -------------------- --
INSERT OVERWRITE TABLE ${dim_db}.dim_carrier
SELECT
  m.carrier_id        AS carrier_id,
  m.carrier_code      AS carrier_code,
  m.carrier_name      AS carrier_name,
  m.carrier_type      AS carrier_type,
  m.country_cd        AS country_cd,
  m.region_cd         AS region_cd,
  m.service_tier      AS service_tier,
  m.max_weight_kg     AS max_weight_kg,
  m.insurance_limit   AS insurance_limit,
  m.status            AS status,
  m.eff_dt            AS eff_dt,
  m.exp_dt            AS exp_dt,
  from_unixtime(unix_timestamp()) AS load_timestamp
FROM ${cs_db}.src_carrier_master m
WHERE 1=1
  ${hivevar:filter_active_status}
  ${hivevar:filter_eff_dt}
  ${hivevar:filter_exp_dt};

-- -------------------- Route dimension -------------------- --
INSERT OVERWRITE TABLE ${dim_db}.dim_route
SELECT
  m.route_id                 AS route_id,
  m.route_code               AS route_code,
  m.route_name               AS route_name,
  m.origin_facility_cd       AS origin_facility_cd,
  m.destination_facility_cd  AS destination_facility_cd,
  m.transit_mode             AS transit_mode,
  m.distance_km              AS distance_km,
  m.standard_transit_days    AS standard_transit_days,
  m.base_cost                AS base_cost,
  m.fuel_surcharge_pct       AS fuel_surcharge_pct,
  m.region_cd                AS region_cd,
  m.status                   AS status,
  m.eff_dt                   AS eff_dt,
  m.exp_dt                   AS exp_dt,
  from_unixtime(unix_timestamp()) AS load_timestamp
FROM ${cs_db}.src_route_master m
WHERE 1=1
  ${hivevar:filter_active_status}
  ${hivevar:filter_eff_dt}
  ${hivevar:filter_exp_dt};
