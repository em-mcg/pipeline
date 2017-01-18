#!/usr/bin/python

import os
import subprocess

from .s3signurl import sign

def get_signed_URI(URI):
    if URI.startswith('s3://'):
        return sign(URI.split('/')[2], '/'.join(split('/')[3:]), os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], https=True, expiry=86400)
    else:
        return URI


def get_duration(URI):
    output = get_all_info(URI)
    iso_time = [v for v in output if "Duration" in v][0].split(',')[0].strip().split(' ')[1]
    return sum(x * int(t) for x, t in zip([3600, 60, 1], iso_time.split('.')[0].split(":")))+float('.'+iso_time.split('.')[1])


def get_all_info(URI):
    results = subprocess.Popen(["ffprobe", URI], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    return results.stdout.readlines()
