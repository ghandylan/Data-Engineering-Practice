import os
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
directory_name = "data"


def get_page_contents(link):
    try:
        page = requests.get(link)
        if page.status_code == 200:
            return page.text
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error! See traceback:\n{e}")
    return None


def get_file_name(page_contents, date):
    file_url = None

    soup = BeautifulSoup(page_contents, 'html.parser')
    for row in soup.find_all("tr"):
        date_cell = row.find("td", align="right")
        try:
            if date_cell.text.strip() == date.strip():
                file_url = row.find("a").get("href")
                break
        # catch-all
        except:  # noqa
            continue
    return file_url


def download_file(file_url):
    if file_url:
        response = requests.get(url + file_url)
        if response.status_code == 200:
            # create 'downloads' folder
            try:
                print("Creating directory...")
                os.mkdir(directory_name)
            # if folder already exists
            except:  # noqa
                print("Directory already exists")

            # save file to 'downloads' folder
            file_name = file_url.split("/")[-1]
            file_path = os.path.join(directory_name, file_name)
            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"File saved to {file_path}")
        else:
            print("Failed to download the file.")
    else:
        print("File with the specified date not found.")


# Clean and convert column to numeric
def extract_numeric(value):
    match = re.search(r'(\d+)', str(value))
    return int(match.group(1)) if match else None


# HourlyDryBulbTemperature
def find_highest_temp(file_name):
    df = pd.read_csv(directory_name + "/" + file_name, low_memory=False)
    df['HourlyDryBulbTemperature'] = df['HourlyDryBulbTemperature'].apply(extract_numeric)
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'],
                                                   errors='coerce')  # Coerce errors to NaN
    df.dropna(subset=['HourlyDryBulbTemperature'], inplace=True)

    temperature = df['HourlyDryBulbTemperature'].max()
    print(f"The highest 'HourlyDryBulbTemperature' is {temperature}")


def main():
    page_contents = get_page_contents(url)
    file_name = get_file_name(page_contents, '2024-01-19 10:36	')
    download_file(file_name)
    find_highest_temp(file_name)


if __name__ == "__main__":
    main()
