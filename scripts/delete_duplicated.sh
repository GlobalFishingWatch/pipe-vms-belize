#!/bin/bash
THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
PROCESS=$(basename $0 .sh)
ARGS=( QUERIED_DATE \
  BQ_SOURCE \
  BQ_DESTINATION )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]} \n"
  echo -e "QUERIED_DATE: The start date of messages you want to download (ex. YYYY-MMM-DD))."
  echo -e "BQ_SOURCE: The path to Bigquery where is the source data (ex. project.dataset.table)."
  echo -e "BQ_DESTINATION: The path to Bigquery where to store the non duplicated records (ex. project.dataset.table)."
}

if [[ $# -ne ${#ARGS[@]} ]]
then
    display_usage
    exit 1
fi

ARG_VALUES=("$@")
PARAMS=()
for index in ${!ARGS[*]}; do
  echo "${ARGS[$index]}=${ARG_VALUES[$index]}"
  declare "${ARGS[$index]}"="${ARG_VALUES[$index]}"
done

#################################################################
# Set envs and buid the GCS_SOURCE
#################################################################
TABLE_DESTINATION="${BQ_DESTINATION}\$${QUERIED_DATE//-/}"
SQL=${ASSETS}/sql/deduplicate.sql.j2
SCHEMA=${ASSETS}/schemas/belize_dedup_schema.json

#################################################################
# Get the non duplicated records
#################################################################
echo "Delete the duplicated records and save to bigquery PARTITIONED <${TABLE_DESTINATION}>"
jinja2 ${SQL} \
  -D source=${BQ_SOURCE//:/.} \
  -D date=${QUERIED_DATE} \
  | bq query \
    --replace \
    --allow_large_results \
    --use_legacy_sql=false \
    --max_rows=0 \
    --time_partitioning_type=DAY \
    --time_partitioning_field=ActualDate \
    --destination_table ${BQ_DESTINATION} \
    --destination_schema ${SCHEMA} \
    --clustering_fields IMEI
RESULT=$?
if [ "${RESULT}" -ne 0 ]
then
  echo "ERROR deleting the duplicted from <${BQ_SOURCE}> to ${TABLE_DESTINATION}."
  exit ${RESULT}
else
  echo "Success: The duplicated records from <${BQ_SOURCE}> were deleted -> PARTITIONED <${TABLE_DESTINATION}>."
fi


################################################################################
# Updates the table description
################################################################################
TABLE_DESC=(
  "* Belize non duplicated messages"
  "*"
  "* Source: ${BQ_SOURCE} "
  "* Date Processed: ${QUERIED_DATE}"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )
bq update --description "${TABLE_DESC}" ${BQ_DESTINATION}
if [ "$?" -ne 0 ]; then
  echo "  Unable to update table description ${BQ_DESTINATION}."
  exit 1
fi
echo "Successfully updated the table description for ${BQ_DESTINATION}."
