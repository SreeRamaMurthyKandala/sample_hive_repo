#!/bin/ksh
###############################################################################
# common_utils.sh - Shared helper functions for HiveGrid shiplogistics jobs
#
# Source this file from every trigger script:
#   . /corp/analytics/shiplogistics/app/common/scripts/common_utils.sh
#
# Variables expected to be set by the caller before sourcing:
#   system               - logical job name (e.g. daily_shipment_ingest)
#   LogDir               - directory for log files
#   DistributionEmail    - notification address
#   mapred_qname         - yarn queue
#   cs_db, uc_db         - source DB and use-case DB
###############################################################################

HIVE_RUNNER="/corp/platform/hive/bin/hive-runner"
HIVE_BIN="hive"

# ----------------------------------------------------------------------------
# log_msg <severity> <message>
#   Writes a timestamped log line to STDOUT (which the trigger redirects to
#   the per-run log file) and to the job's rolling log.
# ----------------------------------------------------------------------------
log_msg() {
  severity=$1
  shift
  msg=$*
  ts=$(date +"%Y-%m-%d.%H:%M:%S")
  printf "%s [%s] [%s] %s\n" "${ts}" "${severity}" "${system}" "${msg}"
}

# ----------------------------------------------------------------------------
# Notify
#   FATAL: emails the distribution list with the failure context, then exits
#   the calling script with status 1. Variables consumed:
#     MailSubject  - subject line of the alert
#     step         - human-readable step name
#     system       - job name
#     sysdate      - YYYYMMDD run date
#     LogDir       - log directory
# ----------------------------------------------------------------------------
Notify() {
  log_msg "ERROR" "Notify called for step='${step}' subject='${MailSubject}'"
  echo -e "This is a system generated message. Job ${system} failed.\nStep: ${step}\nLog: ${LogDir}/${system}_${sysdate}.log\nPlease investigate." \
    | mail -s "${MailSubject}" "${DistributionEmail}"
  exit 1
}

# ----------------------------------------------------------------------------
# Warn
#   Non-fatal warning email. Does NOT exit. Variables consumed:
#     WarnSubject, WarnMessage
# ----------------------------------------------------------------------------
Warn() {
  log_msg "WARN" "Warn called subject='${WarnSubject}' message='${WarnMessage}'"
  echo -e "WARNING from job ${system}.\n${WarnMessage}\nLog: ${LogDir}/${system}_${sysdate}.log" \
    | mail -s "${WarnSubject}" "${DistributionEmail}"
}

# ----------------------------------------------------------------------------
# count_check <count_value> <label>
#   Aborts the job (calls Notify) when the supplied count is empty, zero,
#   negative or non-numeric. Used to enforce "must have at least 1 row"
#   gates after Hive count queries.
# ----------------------------------------------------------------------------
count_check() {
  cnt=$1
  label=$2
  case "${cnt}" in
    ''|*[!0-9-]*)
      step="count_check"
      MailSubject="FAILED: ${system} ${label} returned non-numeric value [${cnt}]"
      Notify
      ;;
  esac
  if [ "${cnt}" -le 0 ]; then
    step="count_check"
    MailSubject="FAILED: ${system} ${label} returned ${cnt} rows"
    Notify
  fi
  log_msg "INFO" "count_check ok ${label}=${cnt}"
}

# ----------------------------------------------------------------------------
# run_hive_count <hql_string>
#   Executes a one-line Hive query that must return a single integer count
#   and prints just that integer to STDOUT.
# ----------------------------------------------------------------------------
run_hive_count() {
  hql=$1
  ${HIVE_BIN} -e "set mapred.job.queue.name=${mapred_qname}; ${hql}" 2>/dev/null | tail -1
}

# ----------------------------------------------------------------------------
# require_file <path>
#   Aborts the job if the file does not exist.
# ----------------------------------------------------------------------------
require_file() {
  f=$1
  if [ ! -f "${f}" ]; then
    step="require_file"
    MailSubject="FAILED: ${system} required file missing: ${f}"
    Notify
  fi
}

# ----------------------------------------------------------------------------
# load_dir_config <config_file>
#   Sources a *_dir.config file into the current shell.
# ----------------------------------------------------------------------------
load_dir_config() {
  cfg=$1
  require_file "${cfg}"
  . "${cfg}"
  log_msg "INFO" "Loaded dir config ${cfg}"
}
