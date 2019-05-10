#!/usr/bin/python

import sys
import os
import subprocess


def run_sprocket_runner(pipespec_file):
    cmd = "nohup python pipeline_runner.py {} input_0:chunked_link:chunks -c test_pipeline_conf.json".format(
        pipespec_file
    )
    print "Running:", cmd
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stdin=subprocess.PIPE)


if __name__ == "__main__":

    test_directory = sys.argv[1]
    test_files = map(lambda f: test_directory + '/' + f,
        list(filter(lambda x: x.startswith('event'), os.listdir(test_directory)))
    )

    for fn in test_files:
        run_sprocket_runner(fn)
