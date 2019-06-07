#!/usr/bin/python
import Queue
import importlib
from time import strftime, localtime

import sprocket.stages
import sprocket.stages.util
from sprocket.scheduler import *
from sprocket.config import settings
from sprocket.controlling.tracker.util import get_default_event
from sprocket.util.durable_queue import DurableQueue
from sprocket.util.misc import read_pem, rand_str


class Pipeline(object):
    class Stage(object):
        def __init__(self, key, stage_name, lambda_function, init_state, config, event, region, delivery_func=None):
            self.key = key
            self.stage_name = stage_name
            self.lambda_function = lambda_function
            self.event = event
            self.init_state = init_state
            self.config = config
            self.region = region
            self.delivery_func = delivery_func
            self.downstream_map = {}
            self.buffer_queues = {}
            self.deliver_queue = Queue.Queue()
            self.context = {}

        def __str__(self):
            return "Stage: %s, init_state: %s, downstreams: %s, delivery_func: %s" % (
                self.key, self.init_state, self.downstream_map.keys(), self.delivery_func)

        def emit(self, src_key, event):
            dstream = self.downstream_map[src_key]
            dstream[1].put({dstream[0]: event})

    def __init__(self, pipe_id=None):
        self.pipe_id = strftime("%Y%m%d%H%M%S", localtime()) + '-' + rand_str(
            4) if pipe_id is None else pipe_id
        self.stages = {}
        self.inputs = {}
        self.outputs = {}
        self.pipedata = {}  # pipeline-wide data shared by stages

        self.scrub_bar_time = 0.0
        self.tasks = []

    def __str__(self):
        return "Pipeline: %s, stages: %s, inputs: %s, outputs: %s" % (
            self.pipe_id, self.stages.keys(), self.inputs.keys(), self.outputs.keys())

    def add_stage(self, stage):
        if stage.key in self.stages:
            raise Exception('Stage key already exists')
        self.stages[stage.key] = stage


    # def add_downstream(self, src_stage, downstream, key):
    #     if not isinstance(src_stage, Pipeline.Stage):
    #         src_stage = self.stages[src_stage]
    #     if isinstance(downstream, basestring):
    #         downstream = self.stages[downstream]
    #     if isinstance(downstream, Pipeline.Stage):
    #         downstream = downstream.buffer_queue
    #     src_stage.downstreams[key] = downstream


def create_from_spec(pipe_spec):
    pipe = Pipeline()

    for node in pipe_spec.get('nodes', []):
        importlib.import_module('sprocket.stages.' + node['stage'])
        init_state = eval('sprocket.stages.' + node['stage']).InitState
        event = node.get('event', get_default_event())
        event['lambda_function'] = node.get('lambda_function', settings['default_lambda_function'])
        func_name = node.get('delivery_function', 'default_delivery_func')
        func_module = importlib.import_module('sprocket.delivery_function.' + func_name)
        pipe.add_stage(Pipeline.Stage(
              key=node['name']
            , stage_name=node['stage']
            , lambda_function=event['lambda_function']
            , init_state=init_state
            , config=node.get('config', {})
            , event=node.get('event', event)
            , region=[node.get('region', settings['default_region'])]
            , delivery_func=getattr(func_module, func_name)
        ))

    for stream in pipe_spec.get('streams', []):
        src_node = stream['src'].split(':')[0]
        src_key = stream['src'].split(':')[1]
        dst_node = stream['dst'].split(':')[0]
        dst_key = stream['dst'].split(':')[1]
        durable = stream.get('durable')

        if src_node is '' or src_key is '' or dst_node is '' or dst_key is '':
            raise Exception('stream format error: %s', stream)

        if src_node.startswith('input'):
            if dst_key not in pipe.stages[dst_node].buffer_queues:
                pipe.stages[dst_node].buffer_queues[dst_key] = DurableQueue() if durable is True else Queue.Queue()
            pipe.inputs[src_node] = (dst_key, pipe.stages[dst_node].buffer_queues[dst_key])  # each input src should only have one key
        elif dst_node.startswith('output'):
            if dst_node not in pipe.outputs:
                pipe.outputs[dst_node] = (dst_key, Queue.Queue())
            pipe.stages[src_node].downstream_map[src_key] = pipe.outputs[dst_node]
        else:
            if src_key in pipe.stages[src_node].downstream_map:
                raise Exception('Source key already exists: %s', stream)
            if dst_key not in pipe.stages[dst_node].buffer_queues:
                pipe.stages[dst_node].buffer_queues[dst_key] = DurableQueue() if durable is True else Queue.Queue()
            pipe.stages[src_node].downstream_map[src_key] = (dst_key, pipe.stages[dst_node].buffer_queues[dst_key])

    return pipe
