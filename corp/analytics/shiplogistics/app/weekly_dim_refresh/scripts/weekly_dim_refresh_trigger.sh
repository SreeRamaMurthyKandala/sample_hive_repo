#!/bin/ksh
###############################################################################
# weekly_dim_refresh_trigger.sh
# Job:        weekly_dim_refresh
# Purpose:    Full refresh of dim_carrier and dim_route from upstream master
#             tables. Backs up the existing dim tables first.
# Schedule:   Sunday 02:00 ET (see event_engine/weekly_dim_refresh_schedule.xml)
# Version:    2026-05-15  Analytics Team  Initial release
###############################################################################

# ---------------------------------------------------------------------------- #
# Bootstrap
# ---------------------------------------------------------------------------- #
system=weekly_dim_refresh
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
log_msg "INFO" "Starting ${system} run_date=${sysdate} load_dt=${load_dt}"
log_msg "INFO" "cs_db=${cs_db} dim_db=${dim_db} queue=${mapred_qname}"
log_msg "INFO" "============================================================"

# ---------------------------------------------------------------------------- #
# Step 1 - Source active-record count check
# ---------------------------------------------------------------------------- #
step="step_1_source_active_count_check"
log_msg "INFO" "Step 1: source active-record check"

active_out=${TmpDir}/${system}_active_${sysdate}.out

${HIVE_RUNNER} \
  -d cs_db=${cs_db} \
  -d mapred_qname=${mapred_qname} \
  -d load_dt="${load_dt}" \
  -f ${HqlDir}/${system}_check.hql \
  > ${active_out} 2>> ${LogDir}/${system}_check_${sysdate}.log

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 1 source active-record check HQL crashed"
  Notify
fi

active_cnt=$(tail -1 ${active_out} | tr -d '[:space:]')
log_msg "INFO" "Combined active master row count = ${active_cnt}"

count_check "${active_cnt}" "active_master_records[${load_dt}]"

# ---------------------------------------------------------------------------- #
# Step 2 - Backup current dim tables to _bkp tables
# ---------------------------------------------------------------------------- #
step="step_2_backup_dim_tables"
log_msg "INFO" "Step 2: backup current dim_carrier and dim_route to _bkp tables"

${HIVE_RUNNER} \
  -d dim_db=${dim_db} \
  -d mapred_qname=${mapred_qname} \
  -f ${HqlDir}/${system}_backup.hql \
  >> ${LogDir}/${system}_backup_${sysdate}.log 2>&1

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 2 backup HQL failed"
  Notify
fi

# Verify the _bkp tables were populated
bkp_carrier_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${dim_db}.dim_carrier_bkp;")
bkp_route_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${dim_db}.dim_route_bkp;")
log_msg "INFO" "dim_carrier_bkp rows=${bkp_carrier_cnt}  dim_route_bkp rows=${bkp_route_cnt}"

case "${bkp_carrier_cnt}" in ''|*[!0-9]*)
  MailSubject="FAILED: ${system} dim_carrier_bkp count is not numeric [${bkp_carrier_cnt}]"
  Notify ;;
esac
case "${bkp_route_cnt}" in ''|*[!0-9]*)
  MailSubject="FAILED: ${system} dim_route_bkp count is not numeric [${bkp_route_cnt}]"
  Notify ;;
esac

# ---------------------------------------------------------------------------- #
# Step 3 - Full refresh of dim_carrier and dim_route
# ---------------------------------------------------------------------------- #
step="step_3_full_refresh_dims"
log_msg "INFO" "Step 3: full refresh of dim_carrier and dim_route"

${HIVE_RUNNER} \
  -i ${ConfigDir}/${system}_filters.config \
  -d cs_db=${cs_db} \
  -d dim_db=${dim_db} \
  -d mapred_qname=${mapred_qname} \
  -d load_dt="${load_dt}" \
  -f ${HqlDir}/${system}_load.hql \
  >> ${LogDir}/${system}_load_${sysdate}.log 2>&1

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 3 load HQL failed"
  Notify
fi

# ---------------------------------------------------------------------------- #
# Step 4 - Post-load count validation
# ---------------------------------------------------------------------------- #
step="step_4_post_load_validation"
log_msg "INFO" "Step 4: post-load count validation"

carrier_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${dim_db}.dim_carrier;")
route_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${dim_db}.dim_route;")

log_msg "INFO" "dim_carrier rows=${carrier_cnt}  dim_route rows=${route_cnt}"

count_check "${carrier_cnt}" "dim_carrier"
count_check "${route_cnt}"   "dim_route"

log_msg "INFO" "============================================================"
log_msg "INFO" "${system} completed successfully"
log_msg "INFO" "  dim_carrier: bkp=${bkp_carrier_cnt} new=${carrier_cnt}"
log_msg "INFO" "  dim_route  : bkp=${bkp_route_cnt} new=${route_cnt}"
log_msg "INFO" "============================================================"

exit 0
