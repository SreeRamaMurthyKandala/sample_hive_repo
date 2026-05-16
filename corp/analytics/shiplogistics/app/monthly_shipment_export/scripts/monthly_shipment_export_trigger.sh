#!/bin/ksh
###############################################################################
# monthly_shipment_export_trigger.sh
# Job:        monthly_shipment_export
# Purpose:    Export the previous month's shipment_summary data via PySpark
#             into a CSV file with HEADER/TRAILER framing, then copy the file
#             to the SFT outbound directory for downstream partners.
# Schedule:   1st of every month at 07:00 ET
# Version:    2026-05-15  Analytics Team  Initial release
###############################################################################

# ---------------------------------------------------------------------------- #
# Bootstrap
# ---------------------------------------------------------------------------- #
system=monthly_shipment_export
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

mkdir -p ${LogDir} ${TmpDir} ${OutboundStaging}
exec >> ${LogDir}/${system}_${sysdate}.log 2>&1

log_msg "INFO" "============================================================"
log_msg "INFO" "Starting ${system} run_dt=${run_dt} export_month=${export_year_month}"
log_msg "INFO" "Output file = ${ExportLocalPath}"
log_msg "INFO" "============================================================"

# ---------------------------------------------------------------------------- #
# Step 1 - Verify the summary table has data for the export month
# ---------------------------------------------------------------------------- #
step="step_1_pre_export_count_check"
log_msg "INFO" "Step 1: pre-export count check"

pre_cnt=$(run_hive_count "SELECT COUNT(*) FROM ${uc_db}.shipment_summary WHERE report_date LIKE '${export_year_month}-%';")
log_msg "INFO" "Rows available for export_month=${export_year_month}: ${pre_cnt}"

count_check "${pre_cnt}" "shipment_summary[${export_year_month}-*]"

# ---------------------------------------------------------------------------- #
# Step 2 - Submit the PySpark export job
# ---------------------------------------------------------------------------- #
step="step_2_spark_submit"
log_msg "INFO" "Step 2: invoking spark-submit"

# Remove any stale output from a previous failed run
rm -f ${ExportLocalPath}

${SparkSubmit} \
  --master yarn \
  --deploy-mode client \
  --queue ${mapred_qname} \
  --driver-memory ${SparkDriverMemory} \
  --executor-memory ${SparkExecutorMemory} \
  --executor-cores ${SparkExecutorCores} \
  --num-executors ${SparkNumExecutors} \
  --conf spark.yarn.queue=${mapred_qname} \
  --conf spark.sql.catalogImplementation=hive \
  --files ${SparkConfDir}/hive-site.xml \
  ${PySparkScript} \
    --hive_db ${uc_db} \
    --dim_db ${dim_db} \
    --export_year_month ${export_year_month} \
    --output_path ${ExportLocalPath} \
    --queue ${mapred_qname} \
    --job_name ${system} \
  >> ${LogDir}/${system}_spark_${sysdate}.log 2>&1

if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 2 spark-submit failed for ${export_year_month}"
  Notify
fi

# ---------------------------------------------------------------------------- #
# Step 3 - Validate the output file exists and has a non-zero line count
# ---------------------------------------------------------------------------- #
step="step_3_validate_output_file"
log_msg "INFO" "Step 3: validating output file"

if [ ! -f "${ExportLocalPath}" ]; then
  MailSubject="FAILED: ${system} Step 3 output file missing: ${ExportLocalPath}"
  Notify
fi

line_cnt=$(wc -l < ${ExportLocalPath} | tr -d '[:space:]')
log_msg "INFO" "Output file line count = ${line_cnt}"

case "${line_cnt}" in
  ''|*[!0-9]*)
    MailSubject="FAILED: ${system} Step 3 line count non-numeric [${line_cnt}]"
    Notify
    ;;
esac

if [ "${line_cnt}" -le 2 ]; then
  # Minimum is HEADER + TRAILER = 2; need at least one data row
  MailSubject="FAILED: ${system} Step 3 output file ${ExportLocalPath} has only ${line_cnt} line(s) (HEADER+TRAILER+data required)"
  Notify
fi

# Check the trailer line is well-formed: TRAILER,<numeric_count>
trailer_line=$(tail -1 ${ExportLocalPath})
case "${trailer_line}" in
  TRAILER,*) log_msg "INFO" "Trailer line OK: ${trailer_line}" ;;
  *)
    MailSubject="FAILED: ${system} Step 3 trailer line malformed: ${trailer_line}"
    Notify
    ;;
esac

# ---------------------------------------------------------------------------- #
# Step 4 - Copy the file to the SFT outbound directory
# ---------------------------------------------------------------------------- #
step="step_4_sft_copy"
log_msg "INFO" "Step 4: copying file to SFT outbound ${SftOutboundDir}"

cp ${ExportLocalPath} ${SftOutboundDir}/${ExportFileName}
if [ $? -gt 0 ]; then
  MailSubject="FAILED: ${system} Step 4 SFT copy failed for ${ExportFileName}"
  Notify
fi

if [ ! -f "${SftOutboundDir}/${ExportFileName}" ]; then
  MailSubject="FAILED: ${system} Step 4 SFT destination file missing"
  Notify
fi

log_msg "INFO" "============================================================"
log_msg "INFO" "${system} completed successfully"
log_msg "INFO" "  file=${SftOutboundDir}/${ExportFileName} lines=${line_cnt}"
log_msg "INFO" "============================================================"

exit 0
