#!/usr/bin/python

import subprocess
import random
import sys
import string
import grpc
import json
import time
import argparse
import boto3
import threading
import os

from datetime import datetime
from sprocket.service import pipeline_pb2_grpc, pipeline_pb2


class Event:

    event_count = 0

    def __init__(self, test_id, device_name):
        self.test_id = test_id
        self.cpu_load = random.randint(0, 100)
        self.device_name = device_name
        self.event_id = Event.event_count
        Event.event_count += 1

    def __str__(self):
        return json.dumps(
            self.as_map(),
            indent=0, sort_keys=True
        )

    def as_map(self):
        return {
            "test_id": self.test_id,
            "cpu_load": self.cpu_load,
            "device_name": self.device_name,
            "event_id": self.event_id
        }

    def make_pipespec(self):
        return {
            "nodes": [
                {
                    "name": "python_cmd",
                    "stage": "python_cmd",
                    "config": {
                        "module": "UpdateAlarmTable",
                        "function": "lambda_handler",
                        "event": self.as_map(),
                        "resources": [
                            {
                                "name": self.device_name,
                                "required_allocation": 1,
                                "max_allocation": 1
                            }
                        ]
                    }
                }
            ],
            "streams": [
                {
                    "src": "input_0:chunked_link",
                    "dst": "python_cmd:chunked_link"
                },
                {
                    "src": "python_cmd:chunked_link",
                    "dst": "output_0:chunks"
                }
            ]
        }


def setup_table_entries(table_name, entries):
    dynamodb = boto3.resource('dynamodb')

    # get all items in the table
    table = dynamodb.Table(table_name)
    items = table.scan()['Items']

    delete_requests = []
    # empty the table
    for item in items:
        delete_request = {
            "DeleteRequest": {
                "Key": {
                    "ID": item['ID']
                }
            }
        }
        delete_requests.append(delete_request)

    # if there were any items, send batch delete request
    if len(delete_requests) > 0:
        dynamodb.batch_write_item(RequestItems={
            table_name: delete_requests
        })

    time.sleep(10)

    put_requests = []
    # add new times with empty values
    for entry in entries:
        put_request = {
            "PutRequest": {
                "Item": {
                    "ID": entry,
                    'values': [],
                    'counter': 0
                }
            }
        }
        put_requests.append(put_request)
    dynamodb.batch_write_item(RequestItems={
        table_name: put_requests
    })
    time.sleep(10)


def invoke_scheduler(pipeline_stub, event, input_streams):
    """
    :param pipeline_stub
    :type pipeline_stub: pipeline_pb2_grpc.PipelineStub
    :param event:
    :type event: Event
    :param input_streams:
    :return:
    """
    submit_request = pipeline_pb2.SubmitRequest(
        pipeline_spec=json.dumps(event.make_pipespec()).encode('utf-8'),
        inputstreams=input_streams
    )
    submit_thread = threading.Thread(target=pipeline_stub.Submit, args=(submit_request, ))
    submit_thread.start()
    return submit_thread


def random_string(length=10):
    letters = string.ascii_lowercase + string.ascii_uppercase
    return ''.join(random.choice(letters) for i in range(length))


def generate_event(device_name, test_id, probability=30):
    if random.random() * 100 < probability:
        return Event(device_name=device_name, test_id=test_id)
    return None


def start_daemon(config, logfile):
    cmd = "python pipeline_daemon.py -c {} --log DEBUG".format(config)
    p = subprocess.Popen(cmd.split(), stderr=logfile, stdout=logfile)
    return p


def is_process_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def run_test(seed, config_fd, num_timesteps, time_step, num_devices, probability):
    test_id = str(int(time.time()))
    test_info = {
        "seed": seed,
        "test_id": test_id,
        "time_step (s)": time_step,
        "num_timesteps": num_timesteps,
        "num_devices": num_devices,
        "probability": probability
    }

    logfile = open("daemon.log", "w+")

    daemon_process = start_daemon(config_fd.name, logfile)
    time.sleep(3)

    if not is_process_running(daemon_process.pid):
        print "Daemon failed to start. Check '{}'".format(logfile.name)
        sys.exit(1)

    print "Started pipeline daemon"
    random.seed(seed)

    table_name = "alarm_table"

    settings = json.loads(config_fd.read())
    print "Random seed is {}. The test_id is '{}'".format(seed, test_id)

    channel = grpc.insecure_channel('%s:%d' % (settings['daemon_addr'], settings['daemon_port']))
    pipeline_stub = pipeline_pb2_grpc.PipelineStub(channel)
    print "Connected to daemon at address {}:{}".format(settings['daemon_addr'], settings['daemon_port'])

    devices = ["device_{}".format(i) for i in range(num_devices)]
    print "Setting up dynamoDB table"
    setup_table_entries(table_name, devices)

    inp = pipeline_pb2.Input()
    inp.uri = "chunks"

    instream = pipeline_pb2.InputStream()
    instream.name = "input_0"
    instream.type = "chunked_list"
    instream.inputs.extend([inp])
    input_streams = [instream]

    threads = []
    device_events = {}
    for d in devices:
        device_events[d] = 0

    try:
        print "Begin generating events"
        for t in range(num_timesteps):
            for d in devices:
                event = generate_event(device_name=d, test_id=test_id, probability=probability)
                if event:
                    # print "Got event:", event
                    thread = invoke_scheduler(pipeline_stub, event, input_streams)
                    threads.append(thread)
                    device_events[d] += 1
            time.sleep(time_step)
        print "Finished sending {} events".format(Event.event_count)
        for t in threads:
            t.join()
        time.sleep(10)
        print "Joined all {} threads".format(Event.event_count)
    except Exception as e:
        print e
    finally:
        kill_process = subprocess.Popen(['/home/ec2-user/pipeline/kill_defunct.sh'])
        kill_process.communicate()

        logfile.write("Run Statistics:\n")
        logfile.write(json.dumps(test_info, indent=4))
        logfile.write(json.dumps(device_events, indent=4))
        logfile.write("\n\n")
        logfile.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="invoke a pipeline from command line")
    parser.add_argument("-s", "--seed", help="random seed", type=int, default=10)
    parser.add_argument("-nts", "--num_timesteps", help="number of time steps", type=int, default=100)
    parser.add_argument("-ts", "--timestep", help="timestep size in seconds", type=float, default=0.0001)
    parser.add_argument("-d", "--devices", help="number of devices", type=int, default=1)
    parser.add_argument("-c", "--config", help="path to config file", type=argparse.FileType('r'),
                        default=open("test_pipeline_conf.json", 'r'))
    parser.add_argument("-p", "--probability", help="probability of generating an event each timestep",
                        type=float, default=10.0)
    args = parser.parse_args()

    run_test(
        seed=args.seed,
        num_devices=args.devices,
        num_timesteps=args.num_timesteps,
        time_step=args.timestep,
        config_fd=args.config,
        probability=args.probability
    )

