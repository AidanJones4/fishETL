from fish_fns import *

bucket_name = "data-eng-resources"
s3_client, bucket = s3_setup(bucket_name)
files = get_fish_file_names(bucket)
super_mean_df = fish_data_means(files, s3_client,bucket_name)
upload_csv(super_mean_df, "AidanJ.csv", "Data26/fish/AidanJ.csv", s3_client, bucket_name)



