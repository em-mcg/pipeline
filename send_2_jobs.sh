#!/bin/bash

nohup python pipeline_runner.py tests/sprocket_test_5_single/event_4.json input_0:chunked_link:chunks -c test_pipeline_conf.json &

#sleep 5

nohup python pipeline_runner.py tests/sprocket_test_5_single/event_1.json input_0:chunked_link:chunks -c test_pipeline_conf.json &


nohup python pipeline_runner.py tests/sprocket_test_5_single/event_2.json input_0:chunked_link:chunks -c test_pipeline_conf.json &
#nohup python pipeline_runner.py pipespec/cmd.pipe input_0:chunked_link:chunks -c test_pipeline_conf.json &
