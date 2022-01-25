import boto3
import json
from pprint import pprint as pp
import pandas as pd

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket_name = "data-eng-resources"

bucket_list = s3_client.list_buckets()
bucket = s3_resource.Bucket(bucket_name)

#for bucket in bucket_list["Buckets"]:
#    print(bucket["Name"])


#bucket_contents = s3_client.list_objects_v2(Bucket = bucket_name, Prefix = "python")


#for object in bucket_contents["Contents"]:
#    print(object["Key"])

contents = bucket.objects.all()

for content in contents:
    pp(content.key)

# s3_object = s3_client.get_object(Bucket = bucket_name, Key="python/chatbot-intent.json")
# strbody = s3_object["Body"].read()
#
# pp(json.loads(strbody))


# s3_object = s3_client.get_object(Bucket = bucket_name, Key="python/happiness-2019.csv")
# pp(s3_object["Body"].read())
# df = pd.read_csv(s3_object["Body"])
# print(df)

dict_to_upload = {"name": "data", "status": 1}

with open("AidanJ.json", "w") as jsonfile:
    json.dump(dict_to_upload, jsonfile)

s3_client.upload_file(Filename="AidanJ.json", Bucket=bucket_name, Key="Data26/Test/AidanJ.json")