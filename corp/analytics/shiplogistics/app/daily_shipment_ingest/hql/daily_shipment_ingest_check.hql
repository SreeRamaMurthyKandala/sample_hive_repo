-- ----------------------------------------------------------------------------
-- daily_shipment_ingest_check.hql
--
-- Pre-load deduplication check. Returns a single integer: the number of
-- (shipment_id, trans_dt) combinations in the source partition that occur
-- more than once. The trigger script treats any non-zero return value as a
-- fatal condition and aborts before the main load runs.
--
-- Variables expected from -d on hive-runner:
--   cs_db, mapred_qname, trans_dt
-- ----------------------------------------------------------------------------

USE ${cs_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;
SET hive.cli.print.header=false;

SELECT
  COUNT(*) AS dup_count
FROM (
  SELECT
    shipment_id,
    trans_dt,
    COUNT(*) AS cnt
  FROM ${cs_db}.src_shipments
  WHERE trans_dt = '${trans_dt}'
    AND shipment_id IS NOT NULL
    AND shipment_id <> ''
  GROUP BY shipment_id, trans_dt
  HAVING COUNT(*) > 1
) dup;
