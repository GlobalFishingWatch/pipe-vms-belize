"""
Converts the XML to JSON

This script will do:
1- Take the content of the XML as input.
2- Converts the XML to JSON.
3- Output a file in JSON format.
"""

import argparse, json, time, xmltodict

def xml2json(xml_path, json_path):
    """
    Converts XML file to JSON file.

    :@param xml: The path to the xml file.
    :@type xml: str.
    :@param json: The path to the json file.
    :@type json: str.
    """
    data_dict = {}
    with open(xml_path) as xml_file:
        data_dict = xmltodict.parse(xml_file.read())

    json_data = json.dumps(data_dict)
    with open(json_path, "w") as json_file:
        json_file.write(json_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converts the XML to JSON')
    parser.add_argument('-xml','--xml_path', help='The input path to the XML.',
                        required=True)
    parser.add_argument('-json','--json_path', help='The output path to the JSON.',
                        required=True)
    args = parser.parse_args()
    xml_path = args.xml_path
    json_path = args.json_path

    start_time = time.time()

    xml2json(xml_path, json_path)

    ### ALL DONE
    print(f"The XML to JSON is done, you can find the output file here: {json_path}")
    print(f"Execution time {(time.time()-start_time)/60} minutes")
