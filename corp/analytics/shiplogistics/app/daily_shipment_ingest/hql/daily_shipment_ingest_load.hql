-- ----------------------------------------------------------------------------
-- daily_shipment_ingest_load.hql
--
-- Main load: INSERT OVERWRITE the daily partition of stg_shipments.
-- Applies the filter set defined in daily_shipment_ingest_filters.config and
-- enriches each row with its most recent shipment event from
-- ${cs_db}.src_shipment_events.
--
-- Variables expected from -d on hive-runner:
--   cs_db, uc_db, mapred_qname, trans_dt, min_declared_value
-- Variables expected from -i (filters file):
--   filter_excl_test_type, filter_excl_internal_type, filter_status_code,
--   filter_min_value, filter_non_null_carrier
-- ----------------------------------------------------------------------------

USE ${uc_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.compress.output=false;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts=-Xmx3072m;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE ${uc_db}.stg_shipments PARTITION (trans_dt)
SELECT
  s.shipment_id              AS shipment_id,
  s.tracking_number          AS tracking_number,
  s.origin_facility_cd       AS origin_facility_cd,
  s.destination_facility_cd  AS destination_facility_cd,
  s.carrier_id               AS carrier_id,
  s.route_id                 AS route_id,
  s.record_type              AS record_type,
  s.shipment_status_code     AS shipment_status_code,
  s.service_level            AS service_level,
  s.booking_channel          AS booking_channel,
  s.customer_id              AS customer_id,
  s.weight_kg                AS weight_kg,
  s.volume_cbm               AS volume_cbm,
  s.declared_value           AS declared_value,
  s.shipping_charge          AS shipping_charge,
  s.pickup_dt                AS pickup_dt,
  s.expected_delivery_dt     AS expected_delivery_dt,
  s.delivery_dt              AS delivery_dt,
  e.event_type_cd            AS last_event_type_cd,
  e.event_facility_cd        AS last_event_facility_cd,
  e.event_ts                 AS last_event_ts,
  from_unixtime(unix_timestamp()) AS load_timestamp,
  s.trans_dt                 AS trans_dt
FROM ${cs_db}.src_shipments s
LEFT OUTER JOIN (
  SELECT
    ev.shipment_id,
    ev.event_type_cd,
    ev.event_facility_cd,
    ev.event_ts,
    ROW_NUMBER() OVER (PARTITION BY ev.shipment_id ORDER BY ev.event_ts DESC) AS rn
  FROM ${cs_db}.src_shipment_events ev
  WHERE ev.event_dt = '${trans_dt}'
) e
ON s.shipment_id = e.shipment_id
AND e.rn = 1
WHERE s.trans_dt = '${trans_dt}'
  AND s.shipment_id IS NOT NULL
  AND s.shipment_id <> ''
  ${hivevar:filter_excl_test_type}
  ${hivevar:filter_excl_internal_type}
  ${hivevar:filter_status_code}
  ${hivevar:filter_min_value}
  ${hivevar:filter_non_null_carrier};
