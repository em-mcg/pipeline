import argparse
import sys
import os
import signal
import logging
import time
import pdb
import ptvsd

from sprocket.config import settings
from sprocket.service import pipeline_server

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s (%(threadName)s) %(name)s:%(lineno)d: %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)

def turn_on_debug():
    import socket
    # Allow other computers to attach to ptvsd at this IP address and port.
    if socket.gethostname() == 'ip-172-31-5-86.us-west-1.compute.internal':
        ptvsd.enable_attach(
            address=('172.31.5.86', 4400), 
            redirect_output=True
        )
        print "Waiting for debug attach"
        # Pause the program until a remote debugger is attached
        ptvsd.wait_for_attach()
        #time.sleep(5)
        print "Received attach"
turn_on_debug()


def start_daemon():
    """A blocking call to start servicing pipeline"""
    try:
        pipeline_server.serve()

        while True:
            time.sleep(3600)  # we need to keep the the only non-daemon thread running
    except KeyboardInterrupt:
        shutdown()


def shutdown(*args):
    logging.info("Shutting down daemon")
    pipeline_server.stop(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", help="logging level", default='INFO')
    args, _ = parser.parse_known_args()
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s (%(threadName)s) %(filename)s:%(lineno)d: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.debug("config: %s", settings)

    try:
        start_daemon()
    except KeyboardInterrupt:
        os._exit(0)


if __name__ == '__main__':
    main()
