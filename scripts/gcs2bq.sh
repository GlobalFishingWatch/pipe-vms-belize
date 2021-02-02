#!/bin/bash
THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
PROCESS=$(basename $0 .sh)
ARGS=( QUERIED_DATE \
  GCS_PATH \
  BQ_PATH )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]} \n"
  echo -e "QUERIED_DATE: The start date of messages you want to download (ex. YYYY-MMM-DD))."
  echo -e "GCS_PATH: The path to the Google Cloud Storage where the downloaded file is stored, (ex: gs://bucket/belize/download)."
  echo -e "BQ_PATH: The path to Bigquery where to store the content of the GCS path (ex. project.dataset.table)."
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
TABLE_DESTINATION="${BQ_PATH}\$${QUERIED_DATE//-/}"
GCS_SOURCE="${GCS_PATH}/${QUERIED_DATE}/${QUERIED_DATE}.json.gz"
CLUSTER_BY=""

#################################################################
# Cleaned the table in case it exists. (--replace)
#################################################################
echo "Clean the partition of the table <${TABLE_DESTINATION}> in case it already exists."
bq rm -f -t "${TABLE_DESTINATION}"
if [ "$?" -ne 0 ]; then
  echo "  ERROR Partition of table <${TABLE_DESTINATION}> can not be removed."
  exit 1
fi
echo "Successfully removed the partition of the table <${TABLE_DESTINATION}>."

#################################################################
# Iterate and Load all JSON from GCS to BQ
#################################################################
echo "Load JSON GZIPPED <${GCS_SOURCE}> to bigquery PARTITIONED <${TABLE_DESTINATION}>"
bq load \
  --source_format=NEWLINE_DELIMITED_JSON \
  --time_partitioning_type=DAY \
  --clustering_fields IMEI \
  ${TABLE_DESTINATION} \
  ${GCS_SOURCE} \
  ${ASSETS}/schemas/belize_schema.json
RESULT=$?
if [ "${RESULT}" -ne 0 ]
then
  echo "ERROR uploading JSON from GCS <${GCS_SOURCE}> to BQ ${TABLE_DESTINATION}."
  exit ${RESULT}
else
  echo "Success: The upload from <${GCS_SOURCE}> -> PARTITIONED <${TABLE_DESTINATION}> was completed."
fi


################################################################################
# Updates the table description
################################################################################
TABLE_DESC=(
  "* Belize API Messages"
  "*"
  "* Source: ${GCS_SOURCE} "
  "* Date Processed: ${QUERIED_DATE}"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )
bq update --description "${TABLE_DESC}" ${BQ_PATH}
if [ "$?" -ne 0 ]; then
  echo "  Unable to update table description ${BQ_PATH}."
  exit 1
fi
echo "Successfully updated the table description for ${BQ_PATH}."
