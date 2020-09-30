"""
Belize API Client

This script will do:
1- Creates a local directory where to download the data.
2- Request the ENDPOINT and download the data in GZIP format.
3- Upload the file to GCS.
"""

from datetime import datetime, timedelta

from google.cloud import storage

from shutil import rmtree

from pathlib import Path

import argparse, linecache, gzip, json, os, re, requests, sys, time


# BELIZE ENDPOINT
SecurityToken = 'fdd361ee-39cc-43b3-9cae-0d522b1e2def'
ClientName = 'Belize+Vessel+Monitoring+%26+Safety+Project'
ENDPOINT = 'https://cssi-tracking.com/WS/WSTrack2.asmx/GetCurrentPositionByClientName'

# FORMATS
FORMAT_DT = '%Y-%m-%d'

# FOLDER
DOWNLOAD_PATH = "download"

def query_data(query_date, wait_time_between_api_calls, file_path, max_retries):
    """
    Queries the Belize API.
    :param query_date: The date to be queried format YYYY-MM-DD.
    :type query_date: str
    :param wait_time_between_api_calls: Time between API calls, seconds.
    :type wait_time_between_api_calls: int
    :param file_path: The absolute path where to store locally the data.
    :type file_path: str
    :param max_retries: The maximum retries to request when an error happens.
    :type max_retries: int
    """
    total=0
    retries=0
    success=False
    # belize_positions_date_url = ENDPOINT + query_date
    belize_positions_date_url = ENDPOINT
    parameters={
        'SecurityToken': f'{SecurityToken}',
        'ClientName': f'{ClientName}'
    }
    headers = {
        'Content-Type': 'text/xml',
        'charset': 'utf-8'
    }
    while retries < max_retries and not success:
        try:
            print('Request to Belize endpoint {}'.format(belize_positions_date_url))
            response = requests.get(belize_positions_date_url, data=parameters, headers=headers)
            if response.status_code == requests.codes.ok:
                data = response.json()
                total += len(data)
                print("The total of array data received is {0}. Retries <{1}>".format(total, retries))

                print('Saving messages to <{}>.'.format(file_path))
                with gzip.open(file_path, 'at', compresslevel=9) as outfile:
                    for message in data:
                        json.dump(message, outfile)
                        outfile.write("\n")
                print('All messages were saved.')
                success=True
            else:
                print("Request did not return successful code: {0} retrying.".format(response.status_code))
                print("Response {0}".format(response))
                retries += 1
        except:
            print('Unknown error')
            exc_type, exc_obj, tb = sys.exc_info()
            f = tb.tb_frame
            lineno = tb.tb_lineno
            filename = f.f_code.co_filename
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
            print('Trying to reconnect in {} segs'.format(wait_time_between_api_calls))
            time.sleep(wait_time_between_api_calls)
            retries += 1
    if not success:
        print('Can not get the Belize data.')
        sys.exit(1)


def create_directory(name):
    """
    Creates a directory in the filesystem.
    :param name: The name of the directory.
    :type name: str
    """
    if not os.path.exists(name):
        os.makedirs(name)

def gcs_transfer(pattern_file, gcs_path):
    """
    Uploads the files from file system to a GCS destination.
    :param pattern_file: The pattern file without wildcard.
    :type pattern_file: str
    :param gcs_path: The absolute path of GCS.
    :type gcs_path: str
    """
    storage_client = storage.Client()
    gcs_search = re.search('gs://([^/]*)/(.*)', gcs_path)
    bucket = storage_client.bucket(gcs_search.group(1))
    pattern_path = Path(pattern_file)
    for filename in pattern_path.parent.glob(pattern_path.name + '*'):
        blob = bucket.blob(gcs_search.group(2) + filename.name)
        blob.upload_from_filename(filename)
        print("File from file system <{}> uploaded to <{}>.".format(filename, gcs_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download all positional data of Belize Vessels for a given day.')
    parser.add_argument('-d','--query_date', help='The date to be queried. Expects a str in format YYYY-MM-DD',
                        required=True)
    parser.add_argument('-o','--output_directory', help='The GCS directory'
                        'where the data will be stored. Expected with slash at'
                        'the end.', required=True)
    parser.add_argument('-wt','--wait_time_between_api_calls', help='Time'
                        'between calls to their API for vessel positions. Measured in'
                        'seconds.', required=False, default=5.0, type=float)
    parser.add_argument('-rtr','--max_retries', help='The amount of retries'
                        'after an error got from the API.', required=False, default=3)
    args = parser.parse_args()
    query_date = datetime.strptime(args.query_date, FORMAT_DT)
    output_directory= args.output_directory
    wait_time_between_api_calls = args.wait_time_between_api_calls
    max_retries = int(args.max_retries)

    file_path = "%s/%s.json.gz" % (DOWNLOAD_PATH, query_date.strftime(FORMAT_DT))

    start_time = time.time()

    create_directory(DOWNLOAD_PATH)

    # Executes the query
    query_data(query_date.strftime(FORMAT_DT), wait_time_between_api_calls, file_path, max_retries)

    # Saves to GCS
    gcs_transfer(file_path, output_directory)

    rmtree(DOWNLOAD_PATH)

    ### ALL DONE
    print("All done, you can find the output file here: {0}".format(output_directory))
    print("Execution time {0} minutes".format((time.time()-start_time)/60))
