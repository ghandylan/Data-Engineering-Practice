import boto3

s3 = boto3.client("s3")


BUCKET_NAME = "commoncrawl"

objectResponse = s3.list_objects_v2(BUCKET_NAME)
for objectDetails in objectResponse["Contents"]:
    print(objectDetails["Name"])