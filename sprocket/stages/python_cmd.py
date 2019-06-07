#!/usr/bin/python
# coding=utf-8

from sprocket.controlling.tracker.machine_state import CommandListState, OnePassState, WaitForInputState
from sprocket.stages import InitStateTemplate, FinalStateTemplate
from sprocket.config import settings
import logging


class FinalState(FinalStateTemplate):
    extra = "(final state)"


class EmitState(OnePassState):
    extra = "(emit state)"
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)

        # TODO: figure out what this does
        self.emit_event('chunked_link', {'key': settings['storage_base'] + 'TEST/'})  # just forward whatever comes in
        

class WaitForNextCommand(WaitForInputState):
    extra = "(wait for input state)"
    nextState = FinalState
    wait_time = 10  # seconds

    commandlist = [
        ("OK:", "wait:{}".format(str(wait_time))),
        ("OK:", "quit:")
    ]

    def __init__(self, prevState):
        super(WaitForNextCommand, self).__init__(prevState)
        # Variables to substitute into the commands
        params = {}

        # Substitute parameters into commands
        self.commands = [
            s.format(**params)
            if s is not None else None
            for s in self.commands
        ]


class RunState(CommandListState):
    extra = "(run state)"
    nextState = WaitForNextCommand
    # Commands to run; lambdas will actually receive the parameters
    # TODO: encapsulate in class?
    # TODO: pipeline streaming requests?
    commandlist = [
        (None, "python_run:{module}:{function}:{event}")
        # (None, 'emit:##TMPDIR## {out_key}'),
    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        # Variables to substitute into the commands
        params = {
            'function': self.config['function'],
            'out_key': settings['storage_base'] + 'TEST/',
            'module': self.config['module'],
            'event': self.config['event']
        }

        # Substitute parameters into commands
        # TODO: make this a function
        self.commands = [s.format(**params)
                         if s is not None else None
                         for s in self.commands]
        logging.debug("RunState: {}".format(self.commands))

    def transition(self, msg):
        # TODO: figure out why msg is in transition
        return super(CommandListState, self).transition(msg)


class InitState(InitStateTemplate):
    extra = "(init state)"
    nextState = RunState
