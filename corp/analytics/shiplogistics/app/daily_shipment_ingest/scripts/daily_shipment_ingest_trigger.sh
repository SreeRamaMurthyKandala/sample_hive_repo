#!/bin/ksh
###############################################################################
# daily_shipment_ingest_trigger.sh
# Job:        daily_shipment_ingest
# Purpose:    Load the previous day's raw shipment records from hivesrcdb into
#             the partitioned staging table shiplogistics_db.stg_shipments.
# Schedule:   Mon-Sat 04:15 ET (see event_engine/daily_shipment_ingest_schedule.xml)
# Version:    2026-05-15  Analytics Team  Initial release
###############################################################################

# ---------------------------------------------------------------------------- #
# Bootstrap: locate config, source common utils, load *_dir.config
# ---------------------------------------------------------------------------- #
system=daily_shipment_ingest
HomeDir=/corp/analytics/shiplogistics/app/${system}
ConfigDir=${HomeDir}/config
CommonHomeDir=/corp/analytics/shiplogistics/app/common

logtm="date +%Y-%m-%d.%H:%M:%S"
sysdate=$(date +"%Y%m%d")

. ${CommonHomeDir}/scripts/common_utils.sh

if [ ! -f "${ConfigDir}/${system}_dir.config" ]; then
  echo "$(${logtm}) ERROR: Config file not found: ${ConfigDir}/${system}_dir.config"
  DistributionEmail=[REDACTED_EMAIL_ADDRESS_1]
  LogDir=/tmp
  MailSubject="FAILED: ${system} Config not found"
  step="bootstrap"
  Notify
fi

. ${ConfigDir}/${system}_dir.config

mkdir -p ${LogDir} ${TmpDir}
exec >> ${LogDir}/${system}_${sysdate}.log 2>&1

log_msg "INFO" "============================================================"
log_msg "INFO" "Starting ${system} run_date=${sysdate} trans_dt=${trans_dt}"
log_msg "INFO" "cs_db=${cs_db} uc_db=${uc_db} queue=${mapred_qname}"
log_msg "INFO" "============================================================"

# ---------------------------------------------------------------------------- #
# Step 1 - Source count check
#   Abort with FATAL if the source partition is empty for trans_dt.
# ---------------------------------------------------------------------------- #
step="step_1_source_count_check"
log_msg "INFO" "Step 1: source count check on ${cs_db}.src_shipments trans_dt=${trans_dt}"

src_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${cs_db}.src_shipments WHERE trans_dt='${trans_dt}';")
log_msg "INFO" "Source row count = ${src_cnt}"

case "${src_cnt}" in
  ''|*[!0-9]*)
    MailSubject="FAILED: ${system} Step 1 source count returned non-numeric [${src_cnt}]"
    Notify
    ;;
esac

if [ "${src_cnt}" -eq 0 ]; then
  MailSubject="FAILED: ${system} No source records in ${cs_db}.src_shipments for ${trans_dt}"
  Notify
fi

# ---------------------------------------------------------------------------- #
# Step 2 - Deduplication pre-check
#   The check HQL returns a single integer: number of duplicate
#   (shipment_id, trans_dt) pairs. Any non-zero value is fatal.
# ---------------------------------------------------------------------------- #
step="step_2_dedup_check"
log_msg "INFO" "Step 2: deduplication pre-check"

dup_out=${TmpDir}/${system}_dedup_${sysdate}.out

${HIVE_RUNNER} \
  -d cs_db=${cs_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -f ${HqlDir}/${system}_check.hql \
  > ${dup_out} 2>> ${LogDir}/${system}_check_${sysdate}.log

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 2 dedup check HQL crashed"
  Notify
fi

dup_cnt=$(tail -1 ${dup_out} | tr -d '[:space:]')
log_msg "INFO" "Duplicate (shipment_id,trans_dt) pair count = ${dup_cnt}"

case "${dup_cnt}" in
  ''|*[!0-9]*)
    MailSubject="FAILED: ${system} Step 2 dedup check returned non-numeric [${dup_cnt}]"
    Notify
    ;;
esac

if [ "${dup_cnt}" -gt 0 ]; then
  MailSubject="FAILED: ${system} ${dup_cnt} duplicate shipments found in source for ${trans_dt}"
  Notify
fi

# ---------------------------------------------------------------------------- #
# Step 3 - Main load via hive-runner
#   -i injects the filter hivevars, -d sets the runtime substitutions, -f
#   runs the load HQL.
# ---------------------------------------------------------------------------- #
step="step_3_main_load"
log_msg "INFO" "Step 3: invoking hive-runner load"

${HIVE_RUNNER} \
  -i ${ConfigDir}/${system}_filters.config \
  -d cs_db=${cs_db} \
  -d uc_db=${uc_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -d min_declared_value=${min_declared_value} \
  -f ${HqlDir}/${system}_load.hql \
  >> ${LogDir}/${system}_load_${sysdate}.log 2>&1

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 3 main load HQL failed"
  Notify
fi

log_msg "INFO" "Main load completed"

# ---------------------------------------------------------------------------- #
# Step 4 - Post-load count validation
#   The staging partition must have at least one row after the load.
# ---------------------------------------------------------------------------- #
step="step_4_post_load_count_check"
log_msg "INFO" "Step 4: post-load count check on ${uc_db}.stg_shipments trans_dt=${trans_dt}"

stg_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${uc_db}.stg_shipments WHERE trans_dt='${trans_dt}';")
log_msg "INFO" "Staging row count = ${stg_cnt}"

count_check "${stg_cnt}" "stg_shipments[${trans_dt}]"

log_msg "INFO" "============================================================"
log_msg "INFO" "${system} completed successfully src=${src_cnt} stg=${stg_cnt}"
log_msg "INFO" "============================================================"

exit 0
