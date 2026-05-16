-- ----------------------------------------------------------------------------
-- daily_shipment_summary_check.hql
--
-- Pre-load check: confirm the staging partition for trans_dt is populated.
-- Returns a single integer count. The trigger script aborts if zero.
--
-- Variables expected from -d on hive-runner:
--   uc_db, mapred_qname, trans_dt
-- ----------------------------------------------------------------------------

USE ${uc_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET mapred.job.queue.name=${mapred_qname};
SET hive.cli.print.header=false;

SELECT
  COUNT(*) AS stg_cnt
FROM ${uc_db}.stg_shipments
WHERE trans_dt = '${trans_dt}';
