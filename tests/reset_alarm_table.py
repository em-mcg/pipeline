#!/home/erin/anaconda3/bin/python

import boto3
import time
from config import TestConfig

'''
Reset all entries in the dynamo DB alarm table with empty
alarm arrays.
'''

TABLE = TestConfig.ALARM_TABLE
ALARM_ENTRIES = TestConfig.ALARM_ENTRIES


def reset_alarm_table():
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodb')

    # get all items in the table
    table = dynamodb.Table(TABLE)
    items = table.scan()['Items']

    delete_requests = []
    # empty alarm table
    for item in items:
        delete_request = {
            "DeleteRequest": {
                "Key": {
                    "ID": item['ID']
                }
            }
        }
        print(delete_request)
        delete_requests.append(delete_request)

    # if there were any items, send batch delete request
    if len(delete_requests) > 0:
        dynamodb.batch_write_item(RequestItems={
            TABLE: delete_requests
        })

    time.sleep(15)

    put_requests = []
    # add new times with empty values
    for i in range(ALARM_ENTRIES):
        alarm_name = "{}_{}".format('alarm', i)
        put_request = {
            "PutRequest": {
                "Item": {
                    "ID": alarm_name,
                    'values': [],
                    'counter': 0
                }
            }
        }
        put_requests.append(put_request)

    dynamodb.batch_write_item(RequestItems={
        TABLE: put_requests
    })


if __name__ == '__main__':
    reset_alarm_table()
