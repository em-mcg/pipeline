#!/usr/bin/python
import sprocket.controlling.worker.worker as worker


def lambda_handler(event, context):
    worker.worker_handler(event, context)
