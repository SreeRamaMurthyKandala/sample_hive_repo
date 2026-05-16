-- ----------------------------------------------------------------------------
-- weekly_dim_refresh_check.hql
--
-- Pre-load source-side health check. Returns a single integer: the count of
-- combined active rows in the carrier and route master sources. The trigger
-- script aborts with a fatal email if this returns zero.
--
-- Variables expected from -d on hive-runner:
--   cs_db, mapred_qname, load_dt
-- ----------------------------------------------------------------------------

USE ${cs_db};

SET mapreduce.job.split.metainfo.maxsize=-1;
SET hive.auto.convert.join=true;
SET mapred.job.queue.name=${mapred_qname};
SET mapreduce.map.memory.mb=2560;
SET mapreduce.map.java.opts=-Xmx1024m;
SET hive.cli.print.header=false;

SELECT
  (carrier_cnt + route_cnt) AS active_cnt
FROM (
  SELECT
    (
      SELECT COUNT(*)
      FROM ${cs_db}.src_carrier_master
      WHERE status='A'
        AND eff_dt <= '${load_dt}'
        AND (exp_dt IS NULL OR exp_dt = '' OR exp_dt >= '${load_dt}')
    ) AS carrier_cnt,
    (
      SELECT COUNT(*)
      FROM ${cs_db}.src_route_master
      WHERE status='A'
        AND eff_dt <= '${load_dt}'
        AND (exp_dt IS NULL OR exp_dt = '' OR exp_dt >= '${load_dt}')
    ) AS route_cnt
) c;
