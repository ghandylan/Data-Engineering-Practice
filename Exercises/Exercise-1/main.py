import os
import shutil
from zipfile import ZipFile, BadZipfile

import requests

# TODO: Try to download files asynchronously using aiohttp and ThreadPoolExecutor
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Tripcurls_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    print("Creating 'downloads' Folder...")
    create_folder()
    print("Downloading Files...")
    download_files()
    print("Extracting Zips...")
    unzip()
    print("Moving MacOS Files to Downloads")
    move_macos_files_to_downloads()
    print("Deleting Zips...")
    delete_zips()


def create_folder():
    try:
        # create downloads directory
        os.mkdir("downloads")
    except Exception as e:
        print(f"'downloads' folder already exists!\n{e}")


def download_files():
    for uri in download_uris:
        try:
            print(f"Downloading file from {uri}")
            uri_response = requests.get(uri)
            # if the URI endpoint is not valid
            if uri_response.status_code == 404:
                print("File not available\n")
                continue
            # if the URI endpoint is not valid
            if uri_response.status_code == 200:
                print("File available, downloading...")
                file_name = get_file_name_from_link(uri)
                file_path = os.path.join("downloads", file_name)
                with open(file_path, "wb") as file:
                    # download and write into disk
                    file.write(uri_response.content)
                    print("File downloaded\n")

        except Exception as e:
            print(e)


def get_file_name_from_link(uri):
    # split uri by forward slashes (/) then get last element for file name
    file_name = "".join(uri.split("/")[3:])
    return file_name


def unzip():
    # get list of files from downloads directory
    zipped_files_names = os.listdir("downloads")
    for zipped_file_name in zipped_files_names:
        # filter by .zip
        if zipped_file_name.endswith('.zip'):
            try:
                with ZipFile(f"downloads\\{zipped_file_name}") as zippedFile:
                    zippedFile.extractall('downloads')
            except BadZipfile as e:
                print(f"{zipped_file_name} might be corrupted. See error: {e}")


# moves files from macos folder to downloads folder, then deletes macos folder
def move_macos_files_to_downloads():
    macos_dir = "downloads\\__MACOSX"
    try:
        macos_files = os.listdir(macos_dir)
    except FileNotFoundError as e:
        print(f"Directory not found: {e}")
        return

    for macos_file in macos_files:
        try:
            shutil.move(os.path.join(macos_dir, macos_file), os.path.join("downloads", macos_file))
        except FileNotFoundError as e:
            print(f"File not found: {e}")

    # delete macos folder
    os.rmdir(macos_dir)


def delete_zips():
    zipped_files_names = os.listdir("downloads")
    for zipped_file_name in zipped_files_names:
        # filter by .zip
        if zipped_file_name.endswith('.zip'):
            try:
                os.remove(f"downloads\\{zipped_file_name}")
            except FileNotFoundError as e:
                print(f"File not found. See error: {e}")


if __name__ == "__main__":
    main()
