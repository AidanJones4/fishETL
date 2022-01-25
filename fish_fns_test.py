from fish_fns import *

s3_client, bucket = s3_setup("data-eng-resources")


def test_get_fish_file_names():

    for file in get_fish_file_names():
        assert file.startswith("python/fish-market") and file.endswith(".csv")


def test_fish_data_means():
    """

    Tests fish_data_means() by independently calculating the sum of all elements and comparing with function output
    """

    files = get_fish_file_names()
    output = fish_data_means(files, "data-eng-resources")

    # Sum of all elements
    elt_sum = 0
    for file in files:

        s3_object = s3_client.get_object(Bucket="data-eng-resources", Key=file)
        data = s3_object["Body"]
        data_numpy = pd.read_csv(data).groupby("Species").mean().to_numpy()                 # Change df to numpy array

        # Loop through elements
        for row in data_numpy:
            for elt in row:
                elt_sum += elt

    assert np.round(np.sum(output.to_numpy()), 6) == np.round(elt_sum/len(files), 6)



