import boto3
import pandas as pd
import numpy as np


def s3_setup(bucket_name):

    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)

    return s3_client, bucket


def get_fish_file_names(bucket):

    contents = bucket.objects.all()
    fish_files = []
    for content in contents:
        if content.key.startswith("python/fish-market"):
            fish_files.append(content.key)

    return fish_files


def fish_data_means(files, s3_client, bucket_name):

    list_of_mean_dfs = []

    for file in files:

        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file)                  # S3 object containing data and metadata of file
        data = s3_object["Body"]                                                        # Retrieve file data

        df = pd.read_csv(data)                                                          # Load data into pandas dataframe

        means = df.groupby("Species").mean()                                            # Group data by species and calculate means of columns for each species

        list_of_mean_dfs.append(means.to_numpy())                                       # Append dataframe to list

    super_mean_df = pd.DataFrame(np.round(np.mean(list_of_mean_dfs, axis=0), 2),        # Calculate the mean across the all days
                                 columns=means.columns.values,
                                 index=means.index.values)

    return super_mean_df


def upload_csv(dataframe, filename, filepath, s3_client, bucket_name):
    dataframe.to_csv(filename)
    s3_client.upload_file(Filename="AidanJ.csv", Bucket=bucket_name, Key=filepath)
    return None

