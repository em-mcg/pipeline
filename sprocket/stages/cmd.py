#!/usr/bin/python
# coding=utf-8

from sprocket.controlling.tracker.machine_state import CommandListState, OnePassState
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
        key = self.in_events.keys()[0]
        self.in_events['chunked_link']['duration'] = 1.0
        #self.in_events['chunked_link']['key'] = None
        self.in_events['chunked_link']['metadata']['duration'] = 1.0
        logging.debug(prevState.recv_queue)
        logging.debug(self.recv_queue)
        print "CMD: EmitState, KEY: {}, VALUE: {}".format(key, self.in_events)

        self.emit_event('chunked_link', {'key': settings['storage_base'] + 'TEST/'})  # just forward whatever comes in
        

class RunState(CommandListState):
    extra = "(run state)"
    nextState = EmitState
    commandlist = [
        (None, "run:{cmd}"),
        (None, 'emit:##TMPDIR## {out_key}'),
        ("OK:", "quit:")]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        print "CMD: RunState __init__"
        print self.config
        params = {'cmd': self.config['cmd'], 'out_key': settings['storage_base'] + 'TEST/' }

        next_commands = []
        for s in self.commands:
            n = s.format(**params) if s is not None else None
            next_commands.append(n)
            logging.debug("{} ; {}; {}".format(s, params, n))

        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]

    def transition(self, msg):
        res = super(CommandListState, self).transition(msg)
        logging.debug("transition: {}".format(self.recv_queue))
        return res


class InitState(InitStateTemplate):
    extra = "(init state)"
    nextState = RunState
