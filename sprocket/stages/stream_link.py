#!/usr/bin/python
import json
import logging
import pdb
import math
import copy
from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message, preprocess_config


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)


class EmitState(OnePassState):
    extra = "(emit)"
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)

    def post_transition(self):

        def send_chunks(i,lineage,framesperchunk, duration):
            while i * (framesperchunk - overlap) / metadata['fps'] < duration and\
                    i != int(config.get('chunklimit', -1)):
                # actual parallelizing here
                newmeta = copy.deepcopy(metadata)
                starttime = float(i * (framesperchunk - overlap)) / float(metadata['fps'])
                end = False
                if starttime + (float(framesperchunk)/float(metadata['fps'])) >= self.local['duration']:
                    end = True
                newmeta['lineage'] = str(lineage + 1)
                newmeta['chunk_duration'] = float(framesperchunk) / float(metadata['fps'])
                self.emit_event('chunked_link', {'metadata': newmeta,
                                             'key': self.in_events['video_link']['key'],
                                             'selector': self.local['selector'],
                                             'starttime': starttime,
                                             'frames': framesperchunk,
                                             'end':end})
                i += 1
                lineage += 1
                '''
                print "---"
                print "starttime:"
                print starttime
                print "framesperchunk:"
                print framesperchunk
                print newmeta
                print "---"
                '''
            return i

        #read in the usual metadata
        metadata = self.in_events['video_link']['metadata']
        config = preprocess_config(self.config,
                                   {'fps': metadata['fps']})
        framesperchunk = float(config.get('framesperchunk', metadata['fps']))  # default to 1 second chunk
        overlap = config.get('overlap', 0)

        #determine how many seconds of streaming to cover up
        streaming_seconds = 20

        #for now, dedicate a lambda per frame for streaming effects 
        streaming_framesperchunk = 1
        i = 0
        lineage = send_chunks(i,i, streaming_framesperchunk, streaming_seconds)

        i = streaming_seconds
        #now for the regular paralellizing
        send_chunks(i,lineage,framesperchunk, self.local['duration'])

        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        output = json.loads(get_output_from_message(self.messages[-1]))
        self.local['duration'] = output['duration']
        self.in_events['video_link']['metadata']['fps'] = output['fps']
        logging.debug("selected format: %s, fps: %d" % (output['format'], output['fps']))
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [(None, 'run:./youtube-dl -j {link} -f "{selector}" > ##TMPDIR##/video_info')
        , ('OK:RETVAL(0', 'run:cat ##TMPDIR##/video_info')
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        default_selector = 'mp4[fps>=0]{user_filter}/webm[fps>=0]{user_filter}/ogg[fps>=0]{user_filter}/' \
                           'flv[fps>=0]{user_filter}/3gp[fps>=0]{user_filter}'  # acceptable formats in preference order
        selector = default_selector.format(**{'user_filter': self.config.get('filter', '')})
        self.local['selector'] = selector
        params = {'link': self.in_events['video_link']['key'],
                  'selector': selector}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]

class InitState(InitStateTemplate):
    nextState = RunState
