-- ----------------------------------------------------------------------------
-- daily_shipment_summary_load.hql
--
-- INNER JOIN staging with both dimension tables and aggregate per
-- (carrier_id, route_id, region_cd, transit_mode, service_level). Computes:
--   total_count        - rows in the group
--   approved_count     - shipments delivered (analogue of "approved")
--   rejected_count     - shipments cancelled or returned
--   total_amount       - sum of declared_value
--   total_weight_kg    - sum of weight_kg
--   approval_rate      - approved_count*100/total_count formatted as "NN.NN%"
--
-- Variables expected from -d on hive-runner:
--   uc_db, dim_db, mapred_qname, trans_dt, report_month
-- Variables expected from -i (filters file):
--   filter_excl_internal_transfer, filter_record_type, filter_joined_carrier
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
SET hive.exec.max.dynamic.partitions=200;
SET hive.exec.max.dynamic.partitions.pernode=200;

INSERT OVERWRITE TABLE ${uc_db}.shipment_summary PARTITION (report_month)
SELECT
  s.trans_dt                                                                AS report_date,
  s.carrier_id                                                              AS carrier_id,
  dc.carrier_name                                                           AS carrier_name,
  s.route_id                                                                AS route_id,
  dr.region_cd                                                              AS region_cd,
  dr.transit_mode                                                           AS transit_mode,
  s.service_level                                                           AS service_level,
  COUNT(*)                                                                  AS total_count,
  SUM(CASE WHEN s.shipment_status_code = 'DELIVERED' THEN 1 ELSE 0 END)     AS approved_count,
  SUM(CASE WHEN s.shipment_status_code IN ('CANCELLED','RETURNED') THEN 1 ELSE 0 END) AS rejected_count,
  SUM(COALESCE(s.declared_value, 0))                                        AS total_amount,
  SUM(COALESCE(s.weight_kg, 0))                                             AS total_weight_kg,
  CONCAT(
    CAST(
      ROUND(
        SUM(CASE WHEN s.shipment_status_code = 'DELIVERED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
      ) AS STRING
    ),
    '%'
  )                                                                          AS approval_rate,
  from_unixtime(unix_timestamp())                                           AS load_timestamp,
  '${report_month}'                                                          AS report_month
FROM ${uc_db}.stg_shipments s
INNER JOIN ${dim_db}.dim_carrier dc
  ON s.carrier_id = dc.carrier_id
INNER JOIN ${dim_db}.dim_route dr
  ON s.route_id = dr.route_id
WHERE s.trans_dt = '${trans_dt}'
  ${hivevar:filter_excl_internal_transfer}
  ${hivevar:filter_record_type}
  ${hivevar:filter_joined_carrier}
GROUP BY
  s.trans_dt,
  s.carrier_id,
  dc.carrier_name,
  s.route_id,
  dr.region_cd,
  dr.transit_mode,
  s.service_level;
