from __future__ import print_function  # Python 2/3 compatibility
import boto3
from botocore.exceptions import ClientError
from timeit import default_timer as timer
from datetime import datetime

import time
import etcd
import traceback
import uuid
import sys

""" event structure
{
  "alarm_name": "alarm_1",
  "data": 1,
  "test_id": "foo",
  "event_id": 0
}

"""

TABLE = 'alarm_table'
MAX_VALS = 500
LAMBDA_TIMEOUT = 5  # seconds
LOCK_TIMEOUT = LAMBDA_TIMEOUT  # seconds

ETCD_HOST = 'ec2-52-53-155-252.us-west-1.compute.amazonaws.com'
ETCD_PORT = 2379
ETCD_LOCK_PREFIX = '/{}/_locks_'.format(TABLE)


def timestamp_now():
    # time in milliseconds
    if sys.version_info >= (3, 0):
        return datetime.now().timestamp() * 1000  # cloudwatch uses this format
    else:
        return (datetime.now() - datetime.fromtimestamp(0)).total_seconds() * 1000


def get_mac_address():
    return ':'.join(['{:02x}'.format((uuid.getnode() >> ele) & 0xff)
                     for ele in range(0, 8 * 6, 8)][::-1])


# add coldstart requests
def create_dynamo_number(num):
    """ Create numeric dynamo entry in dictionary format """
    return {"N": str(num)}


def update_alarm_table(db_client, alarm_name, new_val):
    """ Update the `alarm_name` item in the alarm table with `new_val` """
    # get current item
    response = db_client.get_item(
        TableName=TABLE,
        Key={'ID': {'S': alarm_name}}
    )

    item = response['Item']
    values = item['values']['L']
    counter = int(item['counter']['N'])

    # prepend new value to list
    values = [create_dynamo_number(new_val)] + values
    counter = counter + 1

    # truncate list so we only save first MAX_VALS entries
    values = values[:MAX_VALS]
    item['values']['L'] = values
    item['counter']['N'] = str(counter)

    ## could do some computation on new list here
    time.sleep(0.2)

    # update list in dynamodb
    response = db_client.put_item(
        TableName=TABLE,
        Item=item
    )

    return counter

    # print(json.dumps(item, indent=4))


def lambda_handler(event, context):
    print(get_mac_address())
    if event == {}:
        # warmup event
        return

    print('{} RequestId: {} Time: {} TestId: {} EventId: {} MacAddress: {}'.format(
        'START_HANDLER', context.aws_request_id, timestamp_now(), event['test_id'], event['event_id'],
        get_mac_address()))
    # get dynamo client
    db_client = boto3.client('dynamodb')

    # grab data from lambda event
    alarm_name = event['alarm_name']
    new_value = int(event['data'])

    # create etcd client
    etcd_client = etcd.Client(
        host=ETCD_HOST,
        port=ETCD_PORT,
        lock_prefix=ETCD_LOCK_PREFIX + event['test_id']
    )

    # get per-alarm lock object
    lock = etcd.Lock(etcd_client, alarm_name)
    db_counter = -1

    try:
        print('{} RequestId: {} Time: {}'.format('ATTEMPT_LOCK_ACQUIRE', context.aws_request_id, timestamp_now()))
        lock.acquire(blocking=True, lock_ttl=LOCK_TIMEOUT)
        start = timestamp_now()
        print('{} RequestId: {} Time: {}'.format('LOCK_ACQUIRE', context.aws_request_id, start))
        db_counter = update_alarm_table(db_client, alarm_name, new_value)
    except ClientError as e:
        print("Got error:", e.response['Error']['Message'])
        # raise e
    except Exception as e:
        print("Got error:", e)
        traceback.print_tb(e.__traceback__)
    finally:
        end = timestamp_now()
        print('{} RequestId: {} Time: {} DBCounter: {}'.format('LOCK_RELEASE', context.aws_request_id, end, db_counter))
        lock.release()
        print('{} RequestId: {} Time: {}'.format('END_HANDLER', context.aws_request_id, timestamp_now()))
