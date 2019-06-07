#!/usr/bin/python

import os
import signal
import subprocess
import sys
import traceback
import datetime
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
from threading import Lock
from time import sleep, time
import random
import string
import json
import logging

import boto3

from sprocket.controlling.common.defs import Defs
from sprocket.controlling.common.network import connect_socket
from sprocket.controlling.common.socket_nb import SocketNB
from sprocket.controlling.worker.fd_wrapper import FDWrapper
from sprocket.controlling.common.logger import get_logger
# from sprocket.util.misc import ForkedPdb

s3_client = boto3.client('s3')

logger = get_logger(__file__.split('/')[-1])

###
#  set a value
###
def _do_set(msg, vals, to_int):
    res = msg.split(':', 1)

    if len(res) != 2 or len(res[0]) < 1:
        vals['cmdsock'].enqueue('FAIL(invalid syntax for SET)')
        return False

    retval = 'OK:SET(%s)'
    if to_int:
        retval = 'OK:SETI(%s)'
        try:
            res[1] = int(res[1])
        except ValueError:
            vals['cmdsock'].enqueue('FAIL(could not interpret %s as an integer)' % res[1])
            return False

    vals[res[0]] = res[1]
    vals['cmdsock'].enqueue(retval % res[0])

    return False

do_set = lambda m, v: _do_set(m, v, False)
do_seti = lambda m, v: _do_set(m, v, True)

###
#  get a value
###
def _do_get(msg, vals, get_info):
    if vals.get(msg) is None:
        vals['cmdsock'].enqueue('FAIL(no such variable %s)' % msg)
        return False

    if get_info:
        vals['cmdsock'].enqueue('INFO:%s:%s' % (msg, vals[msg]))
        vals['cmdsock'].enqueue('OK:GETI(%s)' % (msg))
    else:
        vals['cmdsock'].enqueue('OK:GET(%s)' % vals[msg])

    return False

do_get = lambda m, v: _do_get(m, v, False)
do_geti = lambda m, v: _do_get(m, v, True)

###
#  dump entire vals dict
###
def do_dump_vals(_, vals):
    vals['cmdsock'].enqueue('OK:DUMP_VALS:%s' % str(vals))
    return False

###
#  run something in the background
###
def _background(runner, vals, queuemsg):
    sock = None
    if vals['nonblock']:
        (r, w) = os.pipe()
        pid = os.fork()

        if pid != 0:
            os.close(w)
            sock = FDWrapper(r)
            sock.set_blocking(False)

            info = vals.setdefault('runinfo', [])
            info.append((pid, SocketNB(sock)))

            if not vals.get('bg_silent'):
                vals['cmdsock'].enqueue(queuemsg)
            return False

        else:
            def on_sigterm(signum, _):
                p = vals.get('popen')
                if p.poll() is None:
                    try:
                        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                        while True:
                            try:
                                os.waitpid(-p.pid)
                            except:
                                break
                    except:
                        pass
                sys.exit(0)
                
            signal.signal(signal.SIGTERM, on_sigterm)
            os.close(r)
            sock = FDWrapper(w)


    try:
        (donemsg, retval) = runner()
    except:
        donemsg = "FAIL: %s" % traceback.format_exc()
        retval = 1

    logger.debug(donemsg)
    if sock is None:
        if vals.get('cmdsock') is not None:
            if "NEW_TASK" not in donemsg and "WAIT" not in donemsg:
                logger.debug("Enqueuing result in cmdsock")
                vals['cmdsock'].enqueue(donemsg)
            return False
        else:
            # for mode 0 where we don't connect to a command server
            # for mode 0 where we don't connect to a command server
            return donemsg

    else:
        logging.debug("Socket is none")
        msg = SocketNB.format_message(donemsg)
        sock.send(msg)
        sock.close()
        sys.exit(retval)


###
#  tell the client to retrieve a segment from S3
###
def do_retrieve(msg, vals):
    (success, bucket, key, filename) = Defs.make_retrievestring(msg, vals)
    if not success:
        vals['cmdsock'].enqueue('FAIL(could not compute download params)')
        return False

    def ret_helper():
        donemsg = 'OK:RETRIEVE(%s/%s)' % (bucket, key)
        retval = 0
        try:
            s3_client.download_file(bucket, key, filename)
        except:
            donemsg = 'FAIL(retrieving %s:%s->%s from s3:\n%s)' % (bucket, key, filename, traceback.format_exc())
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:RETRIEVING(%s/%s->%s)' % (bucket, key, filename))

###
#  tell the client to upload a segment to s3
###
def do_upload(msg, vals):
    (success, bucket, key, filename) = Defs.make_uploadstring(msg, vals)
    if not success:
        vals['cmdsock'].enqueue('FAIL(could not compute upload params)')
        return False

    def ret_helper():
        donemsg = 'OK:UPLOAD(%s/%s)' % (bucket, key)
        retval = 0
        try:
            s3_client.upload_file(filename, bucket, key)
        except:
            donemsg = 'FAIL(uploading %s->%s:%s to s3:\n%s)' % (filename, bucket, key, traceback.format_exc())
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:UPLOADING(%s->%s/%s)' % (filename, bucket, key))

###
#  echo msg back to the server
###
def do_echo(msg, vals):
    vals['cmdsock'].enqueue('OK:ECHO(%s)' % msg)
    return False

###
#  we've been told to quit
###
def do_quit(_, vals):
    vals['cmdsock'].close()
    for pid, sock in vals.setdefault('runinfo', []):
        try:
            os.kill(pid, signal.SIGTERM)
        except:
            pass
    for pid, sock in vals.setdefault('runinfo', []):
        try:
            os.waitpid(pid, 0)
            sock.close()
        except:
            pass
    return True

###
#  run the command
###
def do_run(msg, vals):
    logger.debug("handler.do_run: msg: {}".format(msg))
    cmdstring = Defs.make_cmdstring(msg, vals)

    def ret_helper():
        retval = 0
        try:
            p = subprocess.Popen([cmdstring], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            vals['popen'] = p
            p.wait()
            retval = p.returncode
            donemsg = 'OK:RETVAL(%d):OUTPUT(%s):COMMAND(%s)' % (retval, p.stdout.read().strip(), cmdstring)
            logger.debug(donemsg)
        except Exception as e:
            donemsg = 'FAIL:RUN %s' % str(e)
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:RUNNING(%s)' % cmdstring)

###
#  run a python command
###
def do_python_run(msg, vals):
    logger.debug("handler.do_python_run")
    cmdstring = Defs.make_cmdstring(msg, vals)

    def ret_helper():
        retval = 0
        try:
            message = msg.strip(":")
            # TODO: fix msg format
            aws_request_id = ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(10)])
            module_name, funct_name, event = message.split(":", 2)
            module = __import__(module_name, fromlist=['object'])
            funct = getattr(module, funct_name)

            # assign random request id
            vals['context'].aws_request_id = aws_request_id
            funct(eval(event), vals['context'])

            # donemsg = 'OK:COMMAND(%s)' % cmdstring
            donemsg = 'OK:RETVAL(%d):COMMAND(%s)' % (retval, cmdstring)
            logger.debug(donemsg)
        except Exception as e:
            traceback.print_exc()
            donemsg = 'FAIL:RUN %s' % str(e)
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:RUNNING(%s)' % cmdstring)


def run_new_task(msg, vals):
    logger.debug("handler.run_new_task")
    cmdstring = Defs.make_cmdstring(msg, vals)

    def ret_helper():
        retval = 0
        try:
            message = msg.strip(":")
            import json
            event = json.loads(message)
            vals['event'] = event
            vals['restart'] = True
            lambda_start_ts = time() if not 'start_ts' in event else event['start_ts']

            logger.debug("Enqueuing command messages")
            vals['cmdsock'].enqueue(json.dumps({'lambda_function': event.get('lambda_function'), 'lambda_start_ts': lambda_start_ts}))
            vals['cmdsock'].enqueue('OK:HELLO')
            vals['cmdsock'].do_write()
            logger.debug("Wrote commands")

            donemsg = 'OK:RETVAL(%d):NEW_TASK(%s)' % (retval, cmdstring)
            logger.debug(donemsg)
        except Exception as e:
            traceback.print_exc()
            donemsg = 'FAIL:RUN %s' % str(e)
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:NEW_TASK(%s)' % cmdstring)

def do_wait(msg, vals):
    logger.debug("handler.do_wait")
    cmdstring = Defs.make_cmdstring(msg, vals)

    def ret_helper():
        retval = 0
        #donemsg = 'OK:RETVAL(0):TIME_OUT(%s)' % cmdstring
        donemsg = 'OK:RETVAL(0):WAIT(%s)' % cmdstring
        try:
            wait_time = float((msg.strip(":")))
            logger.debug("sleeping for {} seconds".format(wait_time))
            event = vals['event']

            lambda_start_ts = time() if not 'start_ts' in event else event['start_ts']

            #logger.debug("Enqueuing command messages")
            #vals['cmdsock'].enqueue(json.dumps({'lambda_function': event.get('lambda_function'), 'lambda_start_ts': lambda_start_ts}))
            #vals['cmdsock'].enqueue('OK:HELLO')
            #vals['cmdsock'].do_write()
            #logger.debug("Wrote commands")

            end_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_time)

            # TODO: probably shouldn't spin
            while True:
                now = datetime.datetime.now()
                vals['cmdsock'].do_read()
                if vals['cmdsock'].want_handle or vals['cmdsock'].want_write:
                    logger.debug("want handle")
                    donemsg = 'OK:RETVAL(0):WAIT(%s)' % cmdstring
                    break
                if now > end_time:
                    logger.debug("wait timeout; now: {}; end: {}".format(now, end_time))
                    #donemsg = 'OK:RETVAL(0):TIME_OUT(%s)' % cmdstring
                    break
                sleep(0.001)

            # vals['cmdsock'].do_write()
            # sleep(wait_time)
            # donemsg = 'OK:RETVAL(0):WAIT(%s)' % cmdstring
        except Exception as e:
            traceback.print_exc()
            donemsg = 'FAIL:WAIT %s' % str(e)
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:WAIT(%s)' % cmdstring)


###
#  connect to peer lambda
###
def do_connect(msg, vals):
    if vals.get('stsock') is not None:
        # already connected
        vals['cmdsock'].enqueue("FAIL(already connected)")
        return False

    try:
        (host, port, tosend) = msg.split(':', 2)
        port = int(port)
        cs = connect_socket(host, port, vals.get('cacert'), vals.get('srvcrt'), vals.get('srvkey'))
    except Exception as e: # pylint: disable=broad-except
        vals['cmdsock'].enqueue('FAIL(%s)' % str(e))
        return False

    if not isinstance(cs, SocketNB):
        vals['cmdsock'].enqueue('FAIL(%s)' % str(cs))
        return True
    vals['stsock'] = cs

    if len(tosend) > 0:
        vals['stsock'].enqueue(tosend)
    vals['cmdsock'].enqueue('OK:CONNECT(%s)' % msg)
    return False

###
#  close connection to peer lambda
###
def do_close_connect(_, vals):
    ns = vals.get('stsock')
    if ns is not None:
        ns.close()
        vals['stsock'] = None

    vals['cmdsock'].enqueue('OK:CLOSE_CONNECT')
    return False


def do_emit(msg, vals):
    """Emit the whole directory to the intermediate store (including filenames if possible)
    msg := local_dir_to_emit URI
    URI := s3://key (key includes bucket name) | redis://key | file://local_dir (file:// needs a relay server or NAT traverse)
    """

    def ret_helper():
        local_dir = msg.split(' ', 1)[0]
        local_dir = local_dir.replace("##TMPDIR##", vals['_tmpdir'])
        local_dir = local_dir.rstrip('/')
    
        protocol = msg.split(' ', 1)[1].split('://', 1)[0]
        key = msg.split(' ', 1)[1].split('://', 1)[1]
    
        filelist = os.listdir(local_dir)
    
        donemsg = 'OK:EMIT(%s), send %d objects' % (msg.split(' ', 1)[0], len(filelist)) # original path is returned

        if protocol == 's3':
            retval = 0
            bucket = key.split('/', 1)[0]
            prefix = key.split('/', 1)[1].rstrip('/')

            if vals.get('threadpool_s3') >= 1:
                working = {}
                finished = {}
                log = []
                lock = Lock()
                def upload(f):
                    with lock:
                        start = working[f] = time()
                        log.append('\n%f: start uploading %s' % (start, f))
                    s3_client.upload_file(local_dir + '/' + f, bucket, prefix + '/' + f)
                    with lock:
                        end = time()
                        finished[f] = end - start
                        log.append('\n%f: finish uploading %s: %f seconds' % (end, f, end - start))

                try:
                    pool = ThreadPool(vals['threadpool_s3'])
                    results = pool.map_async(upload, filelist, chunksize=1)

                    while True:
                        try:
                            res = results.get(0.2)
                            break
                        except TimeoutError:
                            # find out any stragglers:
                            straggler_percentile, straggler_slowdown, straggler_delay = [float(v) for v in vals.get('straggler_configs', '0.9 2 1').split()]
                            stragglers = []
                            with lock:
                                if len(finished) == len(filelist):
                                    break
                                if float(len(finished)) / len(filelist) > straggler_percentile:
                                    average = sum(finished.values()) / len(finished)
                                    current = time()
                                    ongoing = [(tup[0], current-tup[1]) for tup in working.items() if tup[0] not in finished]
                                    donemsg += '\naverage: %f, working: %s' % (average, ongoing)
                                    stragglers = [tup[0] for tup in ongoing if tup[1] > average * straggler_slowdown + straggler_delay]
                                if len(stragglers) > 0:
                                    donemsg += '\nadding speculative upload: %s' % stragglers
                                    res = pool.map_async(upload, stragglers, chunksize=1)

                    donemsg += '\nemit log: ' + str(log)
                except:
                    donemsg = 'FAIL:EMIT(%s->%s\n%s)' % (local_dir, bucket+'/'+prefix+'/...', traceback.format_exc())
                    retval = 1

            else:
                for f in filelist:
                    try:
                        s3_client.upload_file(local_dir+'/'+f, bucket, prefix+'/'+f)
                    except:
                        donemsg = 'FAIL:EMIT(%s->%s\n%s)' % (local_dir, bucket+'/'+prefix+'/'+f, traceback.format_exc())
                        retval = 1
                        break

        elif protocol == 'redis':
            donemsg = 'redis: not implemented yet'
            retval = 1

        elif protocol == 'file':
            donemsg = 'file: not implemented yet'
            retval = 1

        else:
            donemsg = 'FAIL(unknown protocol: %s)' % protocol
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:EMITTING')


def do_collect(msg, vals):
    """Collect the whole directory from the intermediate store (including filenames if possible)
    msg := URI local_dir_to_store
    URI := s3://key (key includes bucket name) | redis://key | file://worker_id/local_dir (file:// needs a relay server or NAT traverse)
    """

    def ret_helper():
        local_dir = msg.split(' ', 1)[1]
        local_dir = local_dir.replace("##TMPDIR##", vals['_tmpdir'])
        local_dir = local_dir.rstrip('/')

        protocol = msg.split(' ', 1)[0].split('://', 1)[0]
        key = msg.split(' ', 1)[0].split('://', 1)[1]

        donemsg = 'OK:COLLECT(%s)' % (msg.split(' ', 1)[1])
        if protocol == 's3':
            retval = 0
            bucket = key.split('/', 1)[0]
            prefix = key.split('/', 1)[1].rstrip('/')

            listed = {}
            try:
                while True:
                    listed = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
                    if listed.has_key('Contents'):
                        break
                    sleep(0.2)

                objectlist = [o['Key'] for o in listed['Contents']]

                if vals.get('threadpool_s3') >= 1:
                    working = {}
                    finished = {}
                    log = []
                    lock = Lock()
                    def download(o):
                        with lock:
                            start = working[o] = time()
                            log.append('%f: start downloading %s' % (start, o))
                        s3_client.download_file(bucket, o, local_dir+'/'+o.split('/')[-1])
                        with lock:
                            end = time()
                            finished[o] = end - start
                            log.append('%f: finish downloading %s: %f seconds' % (end, o, end - start))

                    pool = ThreadPool(vals.get('threadpool_s3', 2))
                    results = pool.map_async(download, objectlist, chunksize=1)

                    while True:
                        try:
                            res = results.get(0.2)
                            break
                        except TimeoutError:
                            straggler_percentile, straggler_slowdown, straggler_delay = [float(v) for v in vals.get('straggler_configs', '0.9 2 1').split()]
                            stragglers = []
                            with lock:
                                if len(finished) == len(objectlist):
                                    break
                                if float(len(finished)) / len(objectlist) > straggler_percentile:
                                    average = sum(finished.values()) / len(finished)
                                    current = time()
                                    ongoing = [(tup[0], current-tup[1]) for tup in working.items() if tup[0] not in finished]
                                    donemsg += '\naverage: %f, working: %s' % (average, ongoing)
                                    stragglers = [tup[0] for tup in ongoing if tup[1] > average * straggler_slowdown + straggler_delay]
                                if len(stragglers) > 0:
                                    donemsg += '\nadding speculative download: %s' % stragglers
                                    res = pool.map_async(download, stragglers, chunksize=1)
                    donemsg += '\ngot %d objects, collect log: %s' % (len(objectlist), log)

                else:
                    for o in objectlist:
                         try:
                             s3_client.download_file(bucket, o, local_dir+'/'+o.split('/')[-1])
                         except:
                             donemsg = 'FAIL:COLLECT(%s->%s\n%s)' % (bucket+'/'+prefix+'/'+o, local_dir, traceback.format_exc())
                             retval = 1
                             break

            except:
                donemsg = 'FAIL:COLLECT(%d objects from %s to %s\n%s)' % (len(listed.get('Contents', [])), 's3://' + bucket + '/...', local_dir, traceback.format_exc())
                retval = 1
            else:
                pass

        elif protocol == 'redis':
            donemsg = 'redis: not implemented yet'
            retval = 1

        elif protocol == 'file':
            donemsg = 'file: not implemented yet'
            retval = 1

        else:
            donemsg = 'FAIL(unknown protocol: %s)' % protocol
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:COLLECTING')


def do_emit_list(msg, vals):
    """Emit the files to keys
    msg := filename1 key1 filename2 key2
    key* := s3://key (includes bucket name) | redis://key | file://local_dir (file:// needs a relay server or NAT traverse)
    """

    file_key_pairs = msg.split(' ')

    for i in xrange(len(file_key_pairs) / 2):
        f = file_key_pairs[i * 2]
        k = file_key_pairs[i * 2 + 1]
        f = f.replace("##TMPDIR##", vals['_tmpdir'])

        try:
            protocol = k.split('://', 1)[0]
            path = k.split('://', 1)[1]
        except:
            logger.error(("k: %s" % k))
            logger.error("file_key_pairs: %s" % file_key_pairs)
            #ForkedPdb().set_trace()

        if protocol == 's3':
            bucket = path.split('/', 1)[0]
            key = path.split('/', 1)[1].rstrip('/')
            try:
                s3_client.upload_file(f, bucket, key)
            except:
                donemsg = 'FAIL:EMIT_LIST(%s->%s: %s)' % (f, k, traceback.format_exc())
                break

        elif protocol == 'redis':
            raise NotImplementedError('redis')

        elif protocol == 'file':
            raise NotImplementedError('file')
        else:
            donemsg = 'FAIL:(unknown protocol: %s)' % protocol
            break
    else:
        donemsg = 'OK:EMIT_LIST(%d files)' % (len(file_key_pairs)/2)

    if vals.get('cmdsock') is not None:
        vals['cmdsock'].enqueue(donemsg)
    return False


def do_collect_list(msg, vals):
    """Collect the keys to files
    msg := key1 filename1 key2 filename2
    key* := s3://key (includes bucket name) | redis://key | file://local_dir (file:// needs a relay server or NAT traverse)
    """
    file_key_pairs = msg.split(' ')

    for i in xrange(len(file_key_pairs) / 2):
        k = file_key_pairs[i * 2]
        f = file_key_pairs[i * 2 + 1]
        f = f.replace("##TMPDIR##", vals['_tmpdir'])

        protocol = k.split('://', 1)[0]
        path = k.split('://', 1)[1]

        if protocol == 's3':
            bucket = path.split('/', 1)[0]
            key = path.split('/', 1)[1].rstrip('/')
            try:
                s3_client.download_file(bucket, key, f)
            except:
                logger.error("bucket: %s, key: %s, f: %s" % (bucket, key, f))
                donemsg = 'FAIL:COLLECT_LIST(%s->%s: %s)' % (k, f, traceback.format_exc())
                break

        elif protocol == 'redis':
            raise NotImplementedError('redis')

        elif protocol == 'file':
            raise NotImplementedError('file')
        else:
            donemsg = 'FAIL(unknown protocol: %s)' % protocol
            break
    else:
        donemsg = 'OK:COLLECT_LIST(%d files)' % (len(file_key_pairs)/2)

    if vals.get('cmdsock') is not None:
        vals['cmdsock'].enqueue(donemsg)

    return False

###
#  dispatch to handler functions
###
message_types = { 'set:': do_set
                , 'seti:': do_seti
                , 'get:': do_get
                , 'geti:': do_geti
                , 'dump_vals:': do_dump_vals
                , 'emit:': do_emit
                , 'collect:': do_collect
                , 'emit_list:': do_emit_list
                , 'collect_list:': do_collect_list
                , 'retrieve:': do_retrieve
                , 'upload:': do_upload
                , 'echo:': do_echo
                , 'quit:': do_quit
                , 'run:': do_run
                , 'python_run': do_python_run
                , 'wait': do_wait
                , 'connect:': do_connect
                , 'close_connect:': do_close_connect
                , 'run_new_task': run_new_task
                }


def handle_message(msg, vals):
    if Defs.debug:
        logger.debug("CLIENT HANDLING %s" % msg)

    for mtype in message_types:
        if msg[:len(mtype)] == mtype:
            return message_types[mtype](msg[len(mtype):], vals)

    # if we got here, we don't recognize the command
    logger.warn("command {} not recognized".format(msg))
    vals['cmdsock'].enqueue("FAIL(no such command '%s')" % msg)
    return False

message_responses = { 'set': 'OK:SET'
                    , 'seti': 'OK:SETI'
                    , 'get': 'OK:GET'
                    , 'geti': 'OK:GETI'
                    , 'dump_vals': 'OK:DUMP_VALS'
                    , 'retrieve': 'OK:RETRIEV'
                    , 'upload': 'OK:UPLOAD'
                    , 'echo': 'OK:ECHO'
                    , 'run': 'OK:R'
                    , 'python_run': 'OK:R'
                    , 'wait': 'OK:R'
                    , 'listen': 'OK:LISTEN'
                    , 'close_listen': 'OK:CLOSE_LISTEN'
                    , 'connect': 'OK:CONNECT'
                    , 'close_connect': 'OK:CLOSE_CONNECT'
                    , 'run_new_task': 'OK:R'
}


def expected_response(msg):
    cmd = msg.split(':', 1)[0]
    expected = message_responses.get(cmd, "OK")
    return expected
