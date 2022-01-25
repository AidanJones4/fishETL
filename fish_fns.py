import boto3
import pandas as pd
import numpy as np
import pymongo
import json


def s3_setup(bucket_name):

    global s3_client
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    global bucket
    bucket = s3_resource.Bucket(bucket_name)

    return s3_client, bucket


def get_fish_file_names():

    contents = bucket.objects.all()
    fish_files = []
    for content in contents:
        if content.key.startswith("python/fish-market") and content.key.endswith(".csv"):
            fish_files.append(content.key)

    return fish_files


def fish_data_means(files, bucket_name):

    list_of_mean_dfs = []

    for file in files:

        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file)                  # S3 object containing data and metadata of file
        data = s3_object["Body"]                                                        # Retrieve file data

        df = pd.read_csv(data)                                                          # Load data into pandas dataframe

        means = df.groupby("Species").mean()                                            # Group data by species and calculate means of columns for each species

        list_of_mean_dfs.append(means.to_numpy())                                       # Append dataframe to list

    super_mean_df = pd.DataFrame(np.mean(list_of_mean_dfs, axis=0),                     # Calculate the mean across the all days
                                 columns=means.columns.values,
                                 index=means.index.values)

    return super_mean_df


def upload_csv(dataframe, filename, filepath, bucket_name):

    dataframe = np.round(dataframe, 2)
    dataframe.to_csv(filename)
    s3_client.upload_file(Filename="AidanJ.csv", Bucket=bucket_name, Key=filepath)

    return None


def connect_ec2(ip):

    client_ec2 = pymongo.MongoClient("mongodb://" + ip + ":27017/Sparta")
    db = client_ec2.fish
    db.species.drop()

    return db


def convert_json(dataframe, filename):
    dataframe = np.round(dataframe, 2)
    dataframe.transpose().to_json(filename)


def upload_json(json_file,db):

    with open(json_file, "r") as data_file:
        data = json.load(data_file)
        for key in data:
            new_dict = {"name": key}
            new_dict.update(data[key])
            db.species.insert_one(new_dict)






