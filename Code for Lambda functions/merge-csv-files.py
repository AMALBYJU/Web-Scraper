import json
import urllib.parse
import boto3
import pandas as pd
from datetime import date

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    list_dfs = []
    for iter in range(event['ArrayProperties']['Size']):
        try:
            file_name = str(iter) + '.csv'
            s3.download_file('web-scraping-sink', 'data/' + file_name, '/tmp/' + file_name)
            s3.delete_object(Bucket='web-scraping-sink', Key='data/' + file_name)
            list_dfs.append(pd.read_csv('/tmp/' + file_name))
        except Exception as e:
            print(e)
            print('Error getting '+ file_name + ' from web-scraping-sink bucket.')
            raise e
    df = pd.concat(list_dfs)
    df.drop_duplicates(inplace = True)
    final_file_name = str(date.today()) + '.csv'
    final_file_path = '/tmp/' + final_file_name
    df.to_csv(final_file_path, index = False)
    s3.upload_file(final_file_path, 'web-scraping-sink', 'data/' + final_file_name)