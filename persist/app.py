import json
import pandas as pd
from db import persist_data
# import requests


def lambda_handler(event, context):
    bucket_name = event['queryStringParameters']['bucketName']
    object_key = event['queryStringParameters']['objectKey']

    df = pd.read_csv(bucket_name, sep=';')

    persist_data(df)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Executed successfully",
        }),
    }


