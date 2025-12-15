import json
import boto3

print('Loading function')

s3 = boto3.client('s3')

def lambda_handler(event, context):

    s3.download_file('web-scraping-sink', 'config/locations.txt', '/tmp/locations.txt')
    with open('/tmp/locations.txt', 'r') as file:
        try:
            x = len(file.readlines())
            print('Total lines:', x)
            return {
                "arraysize": int(x)
            }
        except Exception as e:
            print(e)
            raise e