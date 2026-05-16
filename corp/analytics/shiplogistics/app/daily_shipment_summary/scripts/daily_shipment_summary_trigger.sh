#!/bin/ksh
###############################################################################
# daily_shipment_summary_trigger.sh
# Job:        daily_shipment_summary
# Purpose:    Produce the daily shipment_summary aggregate and write DQ errors
#             into shipment_dq_errors. Sends a non-fatal WARN email if the
#             error rate exceeds err_rate_threshold percent.
# Schedule:   Mon-Sat 05:30 ET, depends on stg_shipments being ready.
# Version:    2026-05-15  Analytics Team  Initial release
###############################################################################

# ---------------------------------------------------------------------------- #
# Bootstrap
# ---------------------------------------------------------------------------- #
system=daily_shipment_summary
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
log_msg "INFO" "Starting ${system} trans_dt=${trans_dt} report_month=${report_month}"
log_msg "INFO" "============================================================"

# ---------------------------------------------------------------------------- #
# Step 1 - Verify staging partition has data for trans_dt
# ---------------------------------------------------------------------------- #
step="step_1_verify_staging_partition"
log_msg "INFO" "Step 1: verifying staging partition ${trans_dt}"

stg_out=${TmpDir}/${system}_stgcnt_${sysdate}.out

${HIVE_RUNNER} \
  -d uc_db=${uc_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -f ${HqlDir}/${system}_check.hql \
  > ${stg_out} 2>> ${LogDir}/${system}_check_${sysdate}.log

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 1 staging partition check HQL crashed"
  Notify
fi

stg_cnt=$(tail -1 ${stg_out} | tr -d '[:space:]')
log_msg "INFO" "Staging rows for ${trans_dt} = ${stg_cnt}"

count_check "${stg_cnt}" "stg_shipments[${trans_dt}]"

# ---------------------------------------------------------------------------- #
# Step 2 - Run the DQ error HQL
# ---------------------------------------------------------------------------- #
step="step_2_dq_errors_load"
log_msg "INFO" "Step 2: writing DQ errors for ${trans_dt}"

${HIVE_RUNNER} \
  -d uc_db=${uc_db} \
  -d dim_db=${dim_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -f ${HqlDir}/${system}_dq.hql \
  >> ${LogDir}/${system}_dq_${sysdate}.log 2>&1

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 2 DQ HQL failed"
  Notify
fi

# ---------------------------------------------------------------------------- #
# Step 3 - Run the summary load HQL
# ---------------------------------------------------------------------------- #
step="step_3_summary_load"
log_msg "INFO" "Step 3: running summary load"

${HIVE_RUNNER} \
  -i ${ConfigDir}/${system}_filters.config \
  -d uc_db=${uc_db} \
  -d dim_db=${dim_db} \
  -d mapred_qname=${mapred_qname} \
  -d trans_dt="${trans_dt}" \
  -d report_month="${report_month}" \
  -f ${HqlDir}/${system}_load.hql \
  >> ${LogDir}/${system}_load_${sysdate}.log 2>&1

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 3 summary load HQL failed"
  Notify
fi

# ---------------------------------------------------------------------------- #
# Step 4 - Post-load validation on both tables
# ---------------------------------------------------------------------------- #
step="step_4_post_load_validation"
log_msg "INFO" "Step 4: post-load count validation"

summary_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${uc_db}.shipment_summary WHERE report_month='${report_month}' AND report_date='${trans_dt}';")
err_cnt=$(run_hive_count    "SELECT COUNT(*) FROM ${uc_db}.shipment_dq_errors WHERE trans_dt='${trans_dt}';")

log_msg "INFO" "summary rows=${summary_cnt}  error rows=${err_cnt}  staging rows=${stg_cnt}"

count_check "${summary_cnt}" "shipment_summary[${trans_dt}]"
case "${err_cnt}" in
  ''|*[!0-9]*)
    MailSubject="FAILED: ${system} Step 4 error count non-numeric [${err_cnt}]"
    Notify
    ;;
esac

# ---------------------------------------------------------------------------- #
# Step 5 - Compute integer error rate and send WARN email if above threshold
# ---------------------------------------------------------------------------- #
step="step_5_error_rate_warning"

# Integer percent: err_cnt * 100 / stg_cnt
err_rate=$(( err_cnt * 100 / stg_cnt ))
log_msg "INFO" "Computed integer error rate = ${err_rate}% (threshold ${err_rate_threshold}%)"

if [ "${err_rate}" -gt "${err_rate_threshold}" ]; then
  WarnSubject="WARN: ${system} DQ error rate ${err_rate}% exceeds threshold ${err_rate_threshold}% for ${trans_dt}"
  WarnMessage="DQ error rate is ${err_rate}% (${err_cnt} errors over ${stg_cnt} staging rows) on ${trans_dt}. This exceeds the configured warning threshold of ${err_rate_threshold}%. The job continued; please review shipment_dq_errors for trans_dt=${trans_dt}."
  Warn
fi

log_msg "INFO" "============================================================"
log_msg "INFO" "${system} completed successfully err_rate=${err_rate}%"
log_msg "INFO" "============================================================"

exit 0
