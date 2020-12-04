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

################################################################################
# Creates the partitioned table if not exists
################################################################################
echo "Validates the existence of the table ${BQ_DESTINATION}"
DATASET=$(echo ${BQ_DESTINATION//:/.} | sed 's/\(.*\)\.[^\.]*$/\1/' | cut -f2 -d.)
TABLE_NAME=$(echo ${BQ_DESTINATION//:/.} | sed 's/.*\.\([^\.]*\)$/\1/')
echo "DATASET=${DATASET}"
echo "TABLE_NAME=${TABLE_NAME}"
QUERY_IF_TABLE_ALREADY_EXISTS=$(bq query --use_legacy_sql=false "SELECT size_bytes FROM ${DATASET}.__TABLES__ WHERE table_id=\"${TABLE_NAME}\"")
if [ "$?" -ne 0 ]; then
  echo "  Unable to verify the existence of the partitioned table for Belize."
  exit 1
fi
echo
if [ -z "${QUERY_IF_TABLE_ALREADY_EXISTS}" ]
then
  echo "  Table ${BQ_DESTINATION} does not exists. Creating the table.."
  bq mk \
    --schema ${SCHEMA} \
    --time_partitioning_type=DAY \
    --time_partitioning_field=ActualDate \
    --clustering_fields IMEI \
    ${BQ_DESTINATION}
  if [ "$?" -ne 0 ]; then
    echo "  (ERROR) Unable to create the partitioned table for research fishing."
    display_usage
    exit 1
  else
    echo "  (DONE). Partitioned table ${BQ_DESTINATION} is already created."
  fi
fi

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
    --destination_table ${TABLE_DESTINATION}
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
