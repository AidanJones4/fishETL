from fish_fns import *

bucket_name = "data-eng-resources"
ip = "18.159.104.166"
s3_setup(bucket_name)
files = get_fish_file_names()
super_mean_df = fish_data_means(files,bucket_name)

#upload_csv(super_mean_df, "AidanJ.csv", "Data26/fish/AidanJ.csv", bucket_name)

database = connect_ec2(ip)
convert_json(super_mean_df, "fish.json")
upload_json("fish.json", database)

#Simple query
print(database.species.find_one({"name": "Parkki"}))





