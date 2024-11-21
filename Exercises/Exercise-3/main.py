import boto3


def download_file_from_s3(bucket_name, object_key, local_path):
    s3 = boto3.client('s3', region_name='us-east-1')
    try:
        s3.download_file(bucket_name, object_key, local_path)
        print(f"Downloaded {object_key} to '{local_path}'")
    except Exception as e:
        print(f"an error occurred {e}")


def main():
    # your code here
    download_file_from_s3('commoncrawl',
                          'crawl-data/CC-MAIN-2022-05/wet.paths.gz',
                          '/tmp/wet.paths.gz')


if __name__ == "__main__":
    main()
