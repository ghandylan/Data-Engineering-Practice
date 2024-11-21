import glob

from pyspark.sql import SparkSession


def main():
    # spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    # your code here
    print(list_files("data", "zip"))


def list_files(directory, extension):
    files = glob.glob(f"{directory}/*.{extension}", recursive=True)
    return files


if __name__ == "__main__":
    main()
