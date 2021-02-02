"""
Belize API Client

This script will do:
1- Creates a local directory where to download the data.
2- Request the ENDPOINT and download the data in XML format.
3- Saves the original XML compress by GZIP.
4- Converts the XML to a NEWLINEJSON and compress with GZIP.
5- Upload the files to GCS.
"""

from datetime import datetime, timedelta

from google.cloud import storage

from shutil import rmtree

from pathlib import Path

from pipe_vms_belize.xml2json import xml2json

import argparse, gzip, html, json, linecache, os, re, requests, sys, time, tempfile


# FORMATS
FORMAT_DT = '%Y-%m-%d'

# FOLDER
DOWNLOAD_PATH = "download"

def query_data(endpoint, wait_time_between_api_calls, file_path, max_retries):
    """
    Queries the Belize API.

    :param endpoint: The endpoint of the Belizean API.
    :type endpoint: str
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
    parameters={
    }
    headers = {
        'Content-Type': 'text/xml',
        'charset': 'utf-8'
    }
    while retries < max_retries and not success:
        try:
            print('Request to Belize endpoint {}'.format(endpoint))
            response = requests.get(endpoint, data=parameters, headers=headers)
            if response.status_code == requests.codes.ok:
                data = response.text
                total += len(data)
                print("The total of array data received is {0}. Retries <{1}>".format(total, retries))

                print('Saving messages to <{}>.'.format(file_path))
                with gzip.open(file_path, 'wt', compresslevel=9) as outfile:
                    outfile.write(f"{data}\n")
                print('All messages from request were saved.')
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

def interpret_bad_formed_xml(xml_gzip_path, json_gzip_path):
    """
    Reads the GZIP xml get from the request, makes it readable, converts to
    a NEWLINEJSON format and compress that JSON using GZIP.

    :param xml_gzip_path: The path to the gzip file that has the xml.
    :type xml_gzip_path: str.
    :param json_gzip_path: The path to the gzip file that will have the JSON.
    :type json_gzip_path: str.
    """
    xml_temp = tempfile.NamedTemporaryFile()
    with open(xml_gzip_path, 'rb') as gzipped:
            data = html.unescape(gzip.decompress(gzipped.read()).decode('utf-8'))
            data_without_carriagereturn = ""
            for line in data:
                data_without_carriagereturn += line.replace('\r','')
            xml_temp.write(data_without_carriagereturn.encode('utf-8'))

    json_temp = tempfile.NamedTemporaryFile()
    xml2json(xml_temp.name, json_temp.name)

    # Have a NEWLINEJSON
    with open(json_temp.name, 'rb') as json_file:
        json_data = json.load(json_file)
        with gzip.open(json_gzip_path, 'wt', compresslevel=9) as outfile:
            for json_line in json_data['string']['NewDataSet']['Table']:
                outfile.write(json.dumps(json_line))
                outfile.write("\n")
    print('Saving messages to <{}>.'.format(json_gzip_path))


def query_and_store(query_data, file_data):
    """
    Queries the endpoint, read the XML and converts to JSON, stores the XML and
    JSON compressed to the GCS.

    :param query_data: A dict with the parameters of the query.
    :type query_data: dict.
    :param file_data: A dict of the file data to convert and storage the results.
    :type file_data: dict.
    """
    # Executes the query to get IMEI list
    query_data(query_data.endpoint_API, query_data.wait_time_between_api_calls, file_data.file_path, query_data.retries)

    # Creates GZIP JSON from GZIP XML
    interpret_bad_formed_xml(file_data.file_path, file_data.json_path)

    # Saves to GCS
    gcs_transfer(file_data.file_path, file_data.output)
    gcs_transfer(file_data.json_path, file_data.output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download all positional data of Belize Vessels for a given day.')
    parser.add_argument('-tk_API','--security_token_API_position_by_clientname', help='The Security Token to access the Belizean WebService Current Positions by ClientName.',
                        required=True)
    parser.add_argument('-tk_API2','--security_token_API_position_by_daterange', help='The Security Token to access the Belizean WebService Positions by Date range.',
                        required=True)
    parser.add_argument('-cli','--client_name', help='The client name to request access to the Belizean WebService Current Position by ClientName',
                        required=True)
    parser.add_argument('-d','--query_date', help='The date to be queried. Expects a str in format YYYY-MM-DD',
                        required=True)
    parser.add_argument('-o','--output_directory', help='The GCS directory'
                        'where the data will be stored. Expected with slash at'
                        'the end.', required=True)
    parser.add_argument('-wt','--wait_time_between_api_calls', help='Time'
                        'between calls to their API for vessel positions. Measured in'
                        'seconds.', required=False, default=5.0, type=float)
    parser.add_argument('-rtr','--max_retries', help='The amount of retries'
                        'after an error got from the API.', required=False, default=1)
    args = parser.parse_args()
    security_token_API1 = args.security_token_API_position_by_clientname
    security_token_API2 = args.security_token_API_position_by_daterange
    client_name = args.client_name
    endpoint_API1 = f'https://cssi-tracking.com/WS/WSTrack2.asmx/GetCurrentPositionByClientName?securitytoken={security_token_API1}&clientname={client_name}'
    query_date = args_query_date
    # Validation of the query_date
    tomorrow = (datetime.strptime(query_date, FORMAT_DT) +
                datetime.timedelta(days=1)).strptime(FORMAT_DT)
    output_directory= args.output_directory
    wait_time_between_api_calls = args.wait_time_between_api_calls
    max_retries = int(args.max_retries)

    file_path = "%s/%s/%s.xml.gz" % (DOWNLOAD_PATH, query_date, query_date)
    json_path = "%s/%s/%s.json.gz" % (DOWNLOAD_PATH, query_date, query_date)

    start_time = time.time()

    create_directory(DOWNLOAD_PATH)

    query_and_store({
            endpoint: endpoint_API1,
            wait_time_between_api_calls: wait_time_between_api_calls,
            retries: max_retries
        },{
            file_path: file_path,
            json_path: json_path,
            output: output_directory
        }
    )

    # Reads the json and extract a list with the unique IMEI
    PATTERN='"IMEI": "([^"]*)"])"'
    gz_file = gzip.GzipFile(json_path, 'rb')
    list_only_imei = re.findall(PATTERN, gz_file.read().decode('utf-8'))
    unique_imei_list = sorted(set(list_only_imei))

    for imei in unique_imei_list:
        imei_start_time = time.time()
        imei_file_path = "%s/%s/%s_%s.xml.gz" % (DOWNLOAD_PATH, query_date, query_date, imei)
        imei_json_path = "%s/%s/%s_%s.json.gz" % (DOWNLOAD_PATH, query_date, query_date, imei)
        # Here we need to read the list of IMEI that was requested and start requesting per each IMEI to the new API.
        endpoint_API2 = f'https://cssi-tracking.com/WS/WSTrack2.asmx/GetPositionsByDateRange?securitytoken={security_token_API2}&imei={imei}&startDate={query_date}&endDate={tomorrow}'
        query_and_store({
                endpoint: endpoint_API2,
                wait_time_between_api_calls: wait_time_between_api_calls,
                retries: max_retries
            },{
                file_path: imei_file_path,
                json_path: imei_json_path,
                output: output_directory
            }
        )
        print(f"IMEI {imei} you can find the output file here: {output_directory}")
        print(f"Execution time {(time.time()-imei_start_time)/60} minutes")
    # Finish with IMEIs list.

    rmtree(DOWNLOAD_PATH)

    ### ALL DONE
    print("All done, you can find the output file here: {0}".format(output_directory))
    print("Execution time {0} minutes".format((time.time()-start_time)/60))
