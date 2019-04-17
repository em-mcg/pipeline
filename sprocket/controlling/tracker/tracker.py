#!/usr/bin/python
import Queue
import errno
import importlib
import json
import logging
import os
import select
import socket
import threading
import time
import signal
import traceback

import sys
import multiprocessing

import sprocket.controlling.common.defs
import sprocket.controlling.tracker.machine_state
import sprocket.controlling.tracker.util
import sprocket.platform
from sprocket.platform.launcher import LaunchEvent
from sprocket.config import settings
from sprocket.controlling.common.network import listen_socket
from sprocket.controlling.common.socket_nb import SocketNB
from sprocket.controlling.tracker.machine_state import TerminalState
from sprocket.controlling.tracker.task import TaskStarter, OrphanedTask
from sprocket.util import lightlog
from sprocket.util.misc import read_pem


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s (%(threadName)s) %(filename)s:%(lineno)d: %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)


class Tracker(object):
    started = False
    started_lock = threading.Lock()
    should_stop = False

    launcher_pid = None
    submitted_queue = Queue.Queue()
    waiting_queues_lock = threading.Lock()
    waiting_queues = {}

    main_thread = None
    invocation_loop_thread = None

    with open(settings['aws_access_key_id_file'], 'r') as f:
        akid = f.read().strip()
    with open(settings['aws_secret_access_key_file'], 'r') as f:
        secret = f.read().strip()

    cacert = read_pem(settings['cacert_file']) if 'cacert_file' in settings else None
    srvcrt = read_pem(settings['srvcrt_file']) if 'srvcrt_file' in settings else None
    srvkey = read_pem(settings['srvkey_file']) if 'srvkey_file' in settings else None

    @classmethod
    def _handle_server_sock(cls, ls, tasks, fd_task_map):
        batched_accept = True
        if batched_accept:
            while True:
                try:
                    (ns, addr) = ls.accept()
                    logging.debug("new conn from addr: %s:%s", addr[0], addr[1])
                except socket.error, e:
                    err = e.args[0]
                    if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                        logging.error("error in accept: %s", e)
                    break
                ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                ns.setblocking(False)

                socknb = SocketNB(ns)
                socknb.do_handshake()

                task_starter = TaskStarter(socknb)
                tasks.append(task_starter)
                fd_task_map[task_starter.current_state.fileno()] = task_starter
        else:
            (ns, addr) = ls.accept()
            logging.debug("new conn from addr: %s:%s", addr[0], addr[1])
            ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            ns.setblocking(False)

            socknb = SocketNB(ns)
            socknb.do_handshake()
            # try:
            #     new_task = Tracker.waiting_queue.get(block=False)  # assume all tasks are the same
            # except Queue.Empty as e:
            #     logging.warning("get response from lambda, but no one's waiting?")
            #     return

            # new_task.start(ns)
            task_starter = TaskStarter(socknb)
            tasks.append(task_starter)
            fd_task_map[task_starter.current_state.fileno()] = task_starter

    @classmethod
    def _main_loop(cls):
        logging.info("tracker listening to port: %d" % settings['tracker_port'])
        lsock = listen_socket(
            '0.0.0.0',
            settings['tracker_port'],
            cls.cacert, cls.srvcrt, cls.srvkey,
            settings['tracker_backlog']
        )
        lsock_fd = lsock.fileno()

        tasks = []
        fd_task_map = {}
        poll_obj = select.poll()
        poll_obj.register(lsock_fd, select.POLLIN)
        npasses_out = 0

        while True:
            if cls.should_stop:
                logging.debug("Tracker stopping main loop")
                Tracker.shutdown_listen_socket(lsock)
                lsock = None
                # os.kill(cls.pylaunch_pid, signal.SIGKILL)

            dflags = []
            for (idx, tsk) in enumerate(tasks):
                st = tsk.current_state
                val = 0
                if st.sock is not None:
                    if not Tracker.is_task_in_terminal_state(tsk):  # always listening
                        val = val | select.POLLIN

                    if st.ssl_write or st.want_write:
                        val = val | select.POLLOUT

                    if val != tsk.rwflag:
                        tsk.rwflag = val
                        dflags.append(idx)
                else:
                    tsk.rwflag = 0
                    dflags.append(idx)
                    if not Tracker.is_task_in_terminal_state(tsk):
                        tsk.current_state = sprocket.controlling.tracker.machine_state.ErrorState(
                            tsk.current_state,
                            "sock closed in %s" % str(tsk)
                        )
                        logging.warning("socket closed abnormally: %s" % str(tsk))

            for idx in dflags:
                cls.register_serverless_socket(tasks[idx], poll_obj)

            pfds = poll_obj.poll(2000)
            npasses_out += 1

            if len(pfds) == 0:
                if cls.should_stop:
                    break
                continue

            # check if any `fd` is available for reading or writing
            for (fd, ev) in pfds:
                if (ev & select.POLLIN) != 0:   # reading
                    cls.handle_fd_read_event(lsock, fd, lsock_fd, tasks, fd_task_map)
                if (ev & select.POLLOUT) != 0:  # writing
                    cls.handle_fd_write_event(fd, fd_task_map)

            terminated_tasks = filter(Tracker.is_task_in_terminal_state, tasks)
            for tsk in terminated_tasks:
                Tracker.cleanup_terminated_task(tsk, poll_obj, fd_task_map)

            # remove all terminated tasks from task list
            tasks = filter(lambda t: not Tracker.is_task_in_terminal_state(t), tasks)
            should_append, removable = cls.process_starter_tasks(tasks, fd_task_map)

            tasks.extend(should_append)
            map(lambda r: tasks.remove(r), removable)
            for tsk in tasks:
                if tsk.current_state.want_handle:
                    if not isinstance(tsk, TaskStarter):
                        tsk.do_handle()

    @classmethod
    def _invocation_loop(cls):
        logging.debug("Tracker _invocation_loop")
        # make sure network is working
        testsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        testsock.connect(("lambda.us-east-1.amazonaws.com", 443))  # incorrect when running on EC2
        addr = testsock.getsockname()[0]
        testsock.close()

        platform_name = settings.get('platform', 'aws_lambda')
        launcher_module = importlib.import_module('sprocket.platform.' + platform_name + '.launcher')
        launcher_cls = launcher_module.Launcher
        launch_queue = multiprocessing.Queue()

        pid = os.fork()
        if pid == 0:
            # child process will run the launcher
            launcher_cls.initialize(launch_queue)
            sys.exit(0)

        cls.launcher_pid = pid

        # cls._invoc_pr = cProfile.Profile()
        # cls._invoc_pr.enable()

        while not cls.should_stop:
            pending = {}  # function_name -> task list

            logging.debug("Tracker _invocation_loop blocking for task")

            t = cls.submitted_queue.get(block=True)  # block till the first task arrives
            while True:
                # transfer `submitted_queue` tasks to the `pending` queue till there are no more submitted tasks
                lst = pending.get(t.lambda_func, [])
                lst.append(t)
                pending[t.lambda_func] = lst
                try:
                    t = cls.submitted_queue.get(block=False)
                except Queue.Empty:
                    break

            # move pending tasks to wait queue
            for pending_function, pending_task_list in pending.iteritems():
                with cls.waiting_queues_lock:
                    wq = cls.waiting_queues.get(pending_function, [])
                    wq.extend(pending_task_list)
                    cls.waiting_queues[pending_function] = wq

            # launch all first task for all ?
            for function_name, task_list in pending.iteritems():
                task_list[0].event['addr'] = settings['daemon_addr']
                start = time.time()
                payload = json.dumps(task_list[0].event)
                logging.debug("Putting {} invokation on launch_queue".format(function_name))

                # launcher is waiting on this queue and will launch events asynchronously
                launch_queue.put(
                    LaunchEvent(
                        nlaunch=len(task_list),
                        fn_name=function_name,
                        akid=cls.akid,
                        secret=cls.secret,
                        payload=payload,
                        regions=task_list[0].regions
                    )
                )

                for task in task_list:
                    # logger = logging.getLogger(p.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                    # logger.debug('%s, %s', p.kwargs['in_events'].values()[0]['metadata']['lineage'], 'send, request')
                    logger = lightlog.getLogger(task.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                    logger.debug(ts=time.time(), lineage=task.kwargs['in_events'].values()[0]['metadata']['lineage'],
                                 op='send', msg='invocation')

                logging.debug("Invoking {} worker(s) takes {} ms".format(len(task_list), (time.time() - start) * 1000))
                logging.debug(task_list)

            logging.debug("Tracker _invocation_loop sleep")
            time.sleep(0.001)
        logging.debug("Tracker _invocation_loop end")

    @staticmethod
    def shutdown_listen_socket(lsock):
        if lsock is not None:
            try:
                lsock.shutdown(0)
                lsock.close()
            except:
                logging.warning("failure shutting down the lsock")
                pass

    @classmethod
    def register_serverless_socket(cls, task, poll_obj):
        if task.rwflag != 0:
            poll_obj.register(task.current_state, task.rwflag)
        else:
            try:
                poll_obj.unregister(task.current_state)
            except Exception as e:
                logging.error("unregister: " + str(e.message))
                pass

    @classmethod
    def handle_fd_read_event(cls, lsock, fd, lsock_fd, tasks, fd_task_map):
        if lsock is not None and fd == lsock_fd:
            # listening socket got an event -> a lambda is requesting a connection to Tracker
            logging.debug("listening sock got conn in")
            cls._handle_server_sock(lsock, tasks, fd_task_map)
        else:
            # socket connected to a lambda got an event -> lambda is sending data
            logging.debug("conn sock %d got buffer readable", fd)
            task = fd_task_map[fd]
            task.do_read()
            logging.debug("{}".format(task.current_state.recv_queue))

    @classmethod
    def handle_fd_write_event(cls, fd, fd_task_map):
        logging.debug("conn sock %d got buffer writable", fd)
        task = fd_task_map[fd]
        task.do_write()
        logging.debug("{}".format(task.current_state.recv_queue))

    @staticmethod
    def is_task_in_terminal_state(task):
        return isinstance(task.current_state, TerminalState)

    @staticmethod
    def cleanup_terminated_task(task, poll_obj, fd_task_map):
        try:
            poll_obj.unregister(task.current_state)
        except Exception as e:
            logging.warning(e.message)
        try:
            task.current_state.close()
        except Exception as e:
            logging.warning(e.message)
        del fd_task_map[task.current_state.fileno()]

    @classmethod
    def process_starter_tasks(cls, tasks, fd_task_map):
        should_append = []
        removable = []
        handleable_starter_tasks = filter(lambda t: t.current_state.want_handle and isinstance(t, TaskStarter), tasks)

        for tsk in handleable_starter_tasks:
            # init msg lets us know which lambda function it's from
            init_msg = tsk.current_state.recv_queue.popleft()
            init_data = json.loads(init_msg)
            with Tracker.waiting_queues_lock:
                # so that we can get Task from the corresponding list
                try:
                    func_queue = Tracker.waiting_queues.get(init_data['lambda_function'], [])
                    #if not len(func_queue):
                    # nothing in function queue yet
                    #    continue

                    real_task = func_queue.pop(0)  # get first task off function queue
                    if len(func_queue) == 0:       # if no more tasks on queue, can delete queue
                        del Tracker.waiting_queues[init_data['lambda_function']]  # GC
                    if 'lambda_start_ts' in init_data:
                        logger = lightlog.getLogger(
                            real_task.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                        logger.debug(ts=init_data['lambda_start_ts'],
                                     lineage=real_task.kwargs['in_events'].values()[0]['metadata']['lineage'],
                                     op='recv', msg='lambda_start_ts')
                except IndexError:
                    real_task = OrphanedTask()  # task doesn't exist
                    logging.info("get an orphaned lambda function, sending quit")

            real_task.rewire(tsk.current_state)                  # transition to a Task
            fd_task_map[tsk.current_state.fileno()] = real_task
            tsk.current_state.update_flags()
            should_append.append(real_task)
            removable.append(tsk)
        return should_append, removable

    @classmethod
    def _start(cls):
        logging.debug("Tracker _start")
        with cls.started_lock:
            if cls.started:
                return

            # pr = cProfile.Profile()
            def profile_main():
                # pr.enable()
                cls._main_loop()
                # pr.disable()
                # pr.dump_stats('tracker_prof_output.cprof')
                # logging.info("tracker_prof_output written")
                logging.info("Tracker: Finish main loop")

            cls.main_thread = threading.Thread(target=profile_main)
            cls.main_thread.setDaemon(False)
            cls.main_thread.start()
            cls.invocation_loop_thread = threading.Thread(target=cls._invocation_loop)
            cls.invocation_loop_thread.setDaemon(True)
            cls.invocation_loop_thread.start()
            cls.started = True

    @classmethod
    def stop(cls):
        cls.should_stop = True

        # kill launcher process
        os.kill(cls.launcher_pid, signal.SIGKILL)
        os.waitpid(cls.launcher_pid, 0)

    @classmethod
    def submit(cls, task):
        if not cls.started:
            # lazy initialization
            # common case will just do this `cls.started` check and avoid acquiring lock in `cls._start`
            cls._start()
        cls.submitted_queue.put(task)

    @classmethod
    def kill(cls, task):
        raise NotImplementedError('kill')
