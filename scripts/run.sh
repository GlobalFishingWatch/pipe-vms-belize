#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P  )"

display_usage() {
  echo "Available Commands"
  echo "  fetch_belize_vms_data        Download BELIZE VMS data to GCS"
  echo "  load_belize_vms_data         Load BELIZE VMS data from GCS to BQ"
  echo "  delete_duplicated            Delete duplicated BELIZE VMS data"
}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi

case $1 in

  fetch_belize_vms_data)
    echo "Running python -m pipe_vms_belize.belize_api_client ${@:2}"
    python -m pipe_vms_belize.belize_api_client ${@:2}
    ;;

  load_belize_vms_data)
    ${THIS_SCRIPT_DIR}/gcs2bq.sh "${@:2}"
    ;;

  delete_duplicated)
    ${THIS_SCRIPT_DIR}/delete_duplicated.sh "${@:2}"
    ;;

  *)
    display_usage
    exit 1
    ;;
esac
