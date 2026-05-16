-- ----------------------------------------------------------------------------
-- daily_shipment_summary_dq.hql
--
-- LEFT JOIN staging with both dimension tables and flag DQ errors:
--   ERR_NO_DIM_CARRIER     - carrier_id has no matching row in dim_carrier
--   ERR_NO_DIM_ROUTE       - route_id has no matching row in dim_route
--   ERR_NEG_VALUE          - declared_value < 0 OR shipping_charge < 0
--   ERR_NULL_MANDATORY     - any of: tracking_number, origin_facility_cd,
--                            destination_facility_cd, weight_kg is NULL
--
-- Each error row is written separately so a single shipment can produce
-- multiple rows (one per failure mode). The error_code column is what the
-- downstream triage dashboard groups by.
--
-- Variables expected from -d on hive-runner:
--   uc_db, dim_db, mapred_qname, trans_dt
-- ----------------------------------------------------------------------------

USE ${uc_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts=-Xmx3072m;

INSERT OVERWRITE TABLE ${uc_db}.shipment_dq_errors PARTITION (trans_dt)
SELECT
  shipment_id,
  tracking_number,
  carrier_id,
  route_id,
  error_code,
  error_description,
  record_type,
  shipment_status_code,
  weight_kg,
  declared_value,
  shipping_charge,
  from_unixtime(unix_timestamp()) AS load_timestamp,
  trans_dt
FROM (

  -- ----------- ERR_NO_DIM_CARRIER -----------
  SELECT
    s.shipment_id            AS shipment_id,
    s.tracking_number        AS tracking_number,
    s.carrier_id             AS carrier_id,
    s.route_id               AS route_id,
    'ERR_NO_DIM_CARRIER'     AS error_code,
    CONCAT('Carrier id [', COALESCE(s.carrier_id,''), '] not present in dim_carrier') AS error_description,
    s.record_type            AS record_type,
    s.shipment_status_code   AS shipment_status_code,
    s.weight_kg              AS weight_kg,
    s.declared_value         AS declared_value,
    s.shipping_charge        AS shipping_charge,
    s.trans_dt               AS trans_dt
  FROM ${uc_db}.stg_shipments s
  LEFT OUTER JOIN ${dim_db}.dim_carrier dc
    ON s.carrier_id = dc.carrier_id
  WHERE s.trans_dt = '${trans_dt}'
    AND dc.carrier_id IS NULL

  UNION ALL

  -- ----------- ERR_NO_DIM_ROUTE -----------
  SELECT
    s.shipment_id            AS shipment_id,
    s.tracking_number        AS tracking_number,
    s.carrier_id             AS carrier_id,
    s.route_id               AS route_id,
    'ERR_NO_DIM_ROUTE'       AS error_code,
    CONCAT('Route id [', COALESCE(s.route_id,''), '] not present in dim_route') AS error_description,
    s.record_type            AS record_type,
    s.shipment_status_code   AS shipment_status_code,
    s.weight_kg              AS weight_kg,
    s.declared_value         AS declared_value,
    s.shipping_charge        AS shipping_charge,
    s.trans_dt               AS trans_dt
  FROM ${uc_db}.stg_shipments s
  LEFT OUTER JOIN ${dim_db}.dim_route dr
    ON s.route_id = dr.route_id
  WHERE s.trans_dt = '${trans_dt}'
    AND dr.route_id IS NULL

  UNION ALL

  -- ----------- ERR_NEG_VALUE -----------
  SELECT
    s.shipment_id            AS shipment_id,
    s.tracking_number        AS tracking_number,
    s.carrier_id             AS carrier_id,
    s.route_id               AS route_id,
    'ERR_NEG_VALUE'          AS error_code,
    CONCAT('Negative monetary value declared_value=', CAST(s.declared_value AS STRING),
           ' shipping_charge=', CAST(s.shipping_charge AS STRING)) AS error_description,
    s.record_type            AS record_type,
    s.shipment_status_code   AS shipment_status_code,
    s.weight_kg              AS weight_kg,
    s.declared_value         AS declared_value,
    s.shipping_charge        AS shipping_charge,
    s.trans_dt               AS trans_dt
  FROM ${uc_db}.stg_shipments s
  WHERE s.trans_dt = '${trans_dt}'
    AND (s.declared_value < 0 OR s.shipping_charge < 0)

  UNION ALL

  -- ----------- ERR_NULL_MANDATORY -----------
  SELECT
    s.shipment_id            AS shipment_id,
    s.tracking_number        AS tracking_number,
    s.carrier_id             AS carrier_id,
    s.route_id               AS route_id,
    'ERR_NULL_MANDATORY'     AS error_code,
    'One or more mandatory fields are NULL (tracking_number, origin/destination facility, weight_kg)' AS error_description,
    s.record_type            AS record_type,
    s.shipment_status_code   AS shipment_status_code,
    s.weight_kg              AS weight_kg,
    s.declared_value         AS declared_value,
    s.shipping_charge        AS shipping_charge,
    s.trans_dt               AS trans_dt
  FROM ${uc_db}.stg_shipments s
  WHERE s.trans_dt = '${trans_dt}'
    AND (
         s.tracking_number IS NULL OR s.tracking_number = ''
      OR s.origin_facility_cd IS NULL OR s.origin_facility_cd = ''
      OR s.destination_facility_cd IS NULL OR s.destination_facility_cd = ''
      OR s.weight_kg IS NULL
    )

) dq_union;
