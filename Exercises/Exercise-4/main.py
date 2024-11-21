# import boto3
import csv
import glob
import json
from typing import IO

from flatten_json import flatten


def list_files(directory, extension):
    files = glob.glob(f"{directory}/**/*.{extension}", recursive=True)
    return files


def flatten_to_csv(file_location_list):
    for file in file_location_list:
        with open(file) as json_file:
            json_data = json.load(json_file)
            flattened_json = flatten(json_data)

            # create a csv based on file name
            raw_file_name = get_raw_file_name(file)
            csv_file = open(raw_file_name + '.csv', 'x', newline='')
            csv_writer = csv.writer(csv_file)

            # write/save flattened json to the csv
            with open(raw_file_name + '.csv', 'w') as gg:
                writer = csv.DictWriter(gg, fieldnames=["g", "g"])  # type: IO[str]
                writer.writerows(flattened_json)

            # for debugging only
            print(file)
            print(f"data type: {type(flattened_json)}")
            print(flattened_json)
            print("\n\n")


def get_raw_file_name(file_name):
    json_file_name = file_name.split("\\")[-1]
    raw_file_name = json_file_name.split(".")[0]
    return raw_file_name


def main():
    # your code here
    jsons = list_files("data", "json")
    flatten_to_csv(jsons)


if __name__ == "__main__":
    main()
