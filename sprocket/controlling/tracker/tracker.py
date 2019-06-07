#!/usr/bin/python
import Queue
import errno
import importlib
import json
import os
import select
import socket
import threading
import time
import signal
import traceback

import sys
import multiprocessing

from sprocket.platform.launcher import LaunchEvent
from sprocket.config import settings
from sprocket.controlling.common.logger import get_logger
from sprocket.controlling.common.network import listen_socket
from sprocket.controlling.common.socket_nb import SocketNB
from sprocket.controlling.tracker.machine_state import TerminalState, WaitForInputState, ErrorState
from sprocket.controlling.tracker.task import TaskStarter, OrphanedTask, Task
from sprocket.util import lightlog
from sprocket.util.misc import read_pem
from typing import List


class SocketPoller:

    def __init__(self, sock):
        self.sock = sock                       # type: listen_socket
        self.fileno = self.sock.fileno()       # type: int
        self.poll_obj = select.poll()          # type: select.poll
        self.poll_obj.register(self.fileno, select.POLLIN)

    def register(self, sock, rw_flag):
        self.poll_obj.register(sock, rw_flag)

    def unregister(self, sock):
        try:
            self.poll_obj.unregister(sock)
        except Exception as e:
            Tracker.logger.error("unregister: " + str(e.message))
            pass

    def poll(self, timeout):
        """
        :param timeout: poll timeout in seconds
        :type timeout: int
        :return: (file descriptor, event type (rw))
        """
        return self.poll_obj.poll(timeout)

    def cleanup(self):
        if self.sock is not None:
            try:
                self.sock.shutdown(0)
                self.sock.close()
            except:
                Tracker.logger.warning("failure shutting down socket with fd {}".format(self.fileno))
                pass
            finally:
                self.sock = None

    def accept(self):
        return self.sock.accept()

    def is_valid(self):
        return self.sock != None


class Tracker(object):
    
    logger = get_logger(__file__.split('/')[-1])

    started = False
    started_lock = threading.Lock()
    should_stop = False

    launcher_pid = None
    submitted_queue = Queue.Queue()
    waiting_queues_lock = threading.Lock()
    waiting_queues = {}

    tasks_waiting_for_input = []
    tasks = []                           # type: List[Task]
    fd_to_task_map = {}

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
    def _handle_server_sock(cls, daemon_socketpoller):
        """
        :type daemon_socketpoller: SocketPoller
        :param tasks:
        :return:
        """
        batched_accept = True
        if batched_accept:
            while True:
                try:
                    # ns :: socket._socketobject, addr :: address of lambda
                    (ns, addr) = daemon_socketpoller.accept()
                    Tracker.logger.debug("new conn from addr: %s:%s", addr[0], addr[1])
                except socket.error, e:
                    err = e.args[0]
                    if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                        Tracker.logger.error("error in accept: %s", e)
                    break
                ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                ns.setblocking(False)

                # new SocketNB object from raw socket
                socknb = SocketNB(ns)

                # authenticate connection
                socknb.do_handshake()

                # init TaskStarter object with the created SocketNB
                task_starter = TaskStarter(socknb)
                cls.tasks.append(task_starter)
                cls.fd_to_task_map[task_starter.current_state.fileno()] = task_starter
                Tracker.logger.debug("fd_task_map: {}".format(cls.fd_to_task_map))
        else:
            (ns, addr) = daemon_socketpoller.accept()
            Tracker.logger.debug("new conn from addr: %s:%s", addr[0], addr[1])
            ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            ns.setblocking(False)

            socknb = SocketNB(ns)
            socknb.do_handshake()
            # try:
            #     new_task = Tracker.waiting_queue.get(block=False)  # assume all tasks are the same
            # except Queue.Empty as e:
            #     Tracker.logger.warning("get response from lambda, but no one's waiting?")
            #     return

            # new_task.start(ns)
            task_starter = TaskStarter(socknb)
            cls.tasks.append(task_starter)
            cls.fd_to_task_map[task_starter.current_state.fileno()] = task_starter

    @classmethod
    def _task_poll_loop(cls):
        Tracker.logger.info("tracker listening to port: %d" % settings['tracker_port'])
        daemon_socketpoller = SocketPoller(listen_socket(
            '0.0.0.0',
            settings['tracker_port'],
            cls.cacert, cls.srvcrt, cls.srvkey,
            settings['tracker_backlog']
        ))

        cls.tasks = []                 # type: List[Task]
        npasses_out = 0

        while True:
            time.sleep(0.1)
            if cls.should_stop:
                # received stop message; end loop and cleanup
                Tracker.logger.debug("Tracker stopping main loop")
                daemon_socketpoller.cleanup()
                break

            # register or unregister task sockets with daemon poll object
            cls.manage_task_registration(daemon_socketpoller, cls.tasks)
            # check for any r/w events on any tack socket
            task_socket_events = daemon_socketpoller.poll(2000)
            npasses_out += 1

            if len(task_socket_events) == 0:
                # received nothing from any lambda; no processing to do
                continue

            # check if any `fd` is available for reading or writing
            for (task_fd, event) in task_socket_events:
                if (event & select.POLLIN) != 0:
                    # task socket received a read event
                    cls.handle_fd_read_event(daemon_socketpoller, task_fd)
                if (event & select.POLLOUT) != 0:
                    # task socket received a write event
                    cls.handle_fd_write_event(task_fd)

            cls.tasks_waiting_for_input = filter(lambda t: t.is_waiting(), cls.tasks)
            terminated_tasks = filter(lambda t: t.is_terminated(), cls.tasks)

            for t in cls.tasks:
                Tracker.logger.debug("Task with socket {} is {}".format(t.current_state.fileno(), t.current_state))

            # remove all terminated tasks from task list
            map(lambda t: Tracker.cleanup_terminated_task(t, daemon_socketpoller), terminated_tasks)
            cls.tasks = filter(lambda t: not Tracker.is_task_in_terminal_state(t), cls.tasks)

            # now process all task starters that can be handled
            starter_tasks = filter(
                lambda t: t.current_state.want_handle and isinstance(t, TaskStarter),
                cls.tasks
            )
            should_append, removable = cls.process_starter_tasks(starter_tasks)

            cls.tasks.extend(should_append)
            map(lambda r: cls.tasks.remove(r), removable)

            # handle the rest of the tasks
            nonstarter_tasks = filter(
                lambda t: t.current_state.want_handle and not isinstance(t, TaskStarter),
                cls.tasks
            )
            for tsk in nonstarter_tasks:
                Tracker.logger.debug("Handling nonstarter task with fileno: {}".format(tsk.current_state.fileno()))
                tsk.do_handle()

    @classmethod
    def _invocation_loop(cls):
        Tracker.logger.debug("Tracker _invocation_loop")
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

            # Tracker.logger.debug("Tracker _invocation_loop blocking for task")

            try:
                t = cls.submitted_queue.get(block=False)
            except Queue.Empty:
                time.sleep(0.001)
                continue

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

                # payload is the event info in the first task on the task list
                payload = json.dumps(task_list[0].event)

                if len(cls.tasks_waiting_for_input):
                    waiter = cls.tasks_waiting_for_input.pop(0)
                    Tracker.logger.debug("Found waiting lambda with socket {}".format(waiter.current_state.sock.fileno()))
                    cls.tasks.remove(waiter)

                    new_task = TaskStarter(SocketNB(waiter.current_state.sock))
                    cls.tasks.append(new_task)
                    Tracker.logger.debug("Updated Task array: {}".format(cls.tasks))
                    cls.fd_to_task_map[new_task.current_state.fileno()] = new_task

                    waiter.current_state.enqueue("run_new_task:{}".format(payload))
                    waiter.current_state.do_write()

                else:
                    Tracker.logger.debug("No waiting lambda found; launching new one")
                    Tracker.logger.debug("Putting {} invocation on launch_queue".format(function_name))
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
                    # Tracker.logger = logging.getTracker.logger(p.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                    # Tracker.logger.debug('%s, %s', p.kwargs['in_events'].values()[0]['metadata']['lineage'], 'send, request')
                    lightlogger = lightlog.getLogger(task.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                    lightlogger.debug(
                        ts=time.time(),
                        lineage=task.kwargs['in_events'].values()[0]['metadata']['lineage'],
                        op='send',
                        msg='invocation'
                    )

                Tracker.logger.debug("Invoking {} worker(s) takes {} ms".format(len(task_list), (time.time() - start) * 1000))

            Tracker.logger.debug("Tracker _invocation_loop sleep")
            time.sleep(0.01)
        Tracker.logger.debug("Tracker _invocation_loop end")

    @staticmethod
    def shutdown_listen_socket(lsock):
        if lsock is not None:
            try:
                lsock.shutdown(0)
                lsock.close()
            except:
                Tracker.logger.warning("failure shutting down the lsock")
                pass

    @classmethod
    def manage_task_registration(cls, poller, tasks):
        """
        :type poller: SocketPoller
        :type tasks: List[Task]
        :return: None
        """
        tasks_to_register = []
        tasks_to_unregister = []
        for (idx, task) in enumerate(tasks):
            if task.current_state.sock is not None:
                rwflag = cls.get_task_rwflag(task)

                if rwflag != task.rwflag:
                    task.rwflag = rwflag
                    tasks_to_register.append(idx)
            else:
                # socket is None
                task.rwflag = 0
                tasks_to_unregister.append(idx)
                if not Tracker.is_task_in_terminal_state(task):
                    task.current_state = ErrorState(
                        task.current_state,
                        "sock closed in %s" % str(task)
                    )
                    Tracker.logger.warning("socket closed abnormally: %s" % str(task))

        map(lambda i: poller.unregister(tasks[i].current_state), tasks_to_unregister)
        map(lambda i: poller.register(tasks[i].current_state, tasks[i].rwflag), tasks_to_register)

    @classmethod
    def get_task_rwflag(cls, task):
        state = task.current_state
        rwflag = 0
        if state.sock is not None:
            if not task.is_terminated():  # always listening
                # There is data to read
                rwflag = rwflag | select.POLLIN

            if state.ssl_write or state.want_write:
                # Ready for output: writing will not block
                rwflag = rwflag | select.POLLOUT
        return rwflag


    @classmethod
    def handle_fd_read_event(cls, daemon_socketpoller, fd):
        """
        :type daemon_socketpoller: SocketPoller
        :param fd:
        :param tasks:
        :param fd_task_map:
        :return:
        """
        if daemon_socketpoller.is_valid() and fd == daemon_socketpoller.fileno:
            # listening socket got an event -> a lambda is requesting a connection to Tracker
            Tracker.logger.debug("listening sock got conn in")
            cls._handle_server_sock(daemon_socketpoller)
        else:
            # socket connected to a lambda got an event -> lambda is sending data
            Tracker.logger.debug("conn sock %d got buffer readable", fd)
            task = cls.fd_to_task_map[fd]
            task.do_read()
            Tracker.logger.debug("recv_queue: {}".format(task.current_state.recv_queue))

    @classmethod
    def handle_fd_write_event(cls, fd):
        Tracker.logger.debug("conn sock %d got buffer writable", fd)
        task = cls.fd_to_task_map[fd]
        task.do_write()
        Tracker.logger.debug("send_queue: {}".format(task.current_state.send_queue))

    @staticmethod
    def is_task_in_terminal_state(task):
        return isinstance(task.current_state, TerminalState)

    @staticmethod
    def is_task_in_waiting_state(task):
        # Tracker.logger.debug(str(task) + " is waiting: " + str(isinstance(task.current_state, WaitForInputState)))
        return isinstance(task.current_state, WaitForInputState)

    @classmethod
    def cleanup_terminated_task(cls, task, socket_poller):
        Tracker.logger.debug("Cleaning up task with fileno {}".format(task.current_state.fileno()))
        try:
            socket_poller.unregister(task.current_state)
        except Exception as e:
            Tracker.logger.warning(e.message)
        try:
            task.current_state.close()
        except Exception as e:
            Tracker.logger.warning(e.message)
        del cls.fd_to_task_map[task.current_state.fileno()]

    @classmethod
    def process_starter_tasks(cls, starter_tasks):
        should_append = []
        removable = []

        Tracker.logger.debug("Processing starter tasks")
        for tsk in starter_tasks:
            Tracker.logger.debug("Starter task with fd {}".format(tsk.current_state.fileno()))
            # init msg lets us know which lambda function it's from

            while True:
                try:
                    init_msg = tsk.current_state.recv_queue.popleft()
                    init_data = json.loads(init_msg)
                except ValueError as e:
                    Tracker.logger.debug("Throwing out message {}".format(init_msg))
                    continue
                break

            Tracker.logger.debug("{}:{} has init_msg {}".format(tsk.current_state.fileno(), tsk, init_msg))
            with Tracker.waiting_queues_lock:
                # so that we can get Task from the corresponding list
                try:
                    func_queue = Tracker.waiting_queues.get(init_data['lambda_function'], [])
                    #if not len(func_queue):
                    # nothing in function queue yet
                    #    continue

                    # get first task off function queue
                    real_task = func_queue.pop(0)
                    if len(func_queue) == 0:
                        # if no more tasks on queue, can delete queue
                        del Tracker.waiting_queues[init_data['lambda_function']]  # garbage collection
                    if 'lambda_start_ts' in init_data:
                        lightlogger = lightlog.getLogger(
                            real_task.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                        lightlogger.debug(ts=init_data['lambda_start_ts'],
                                          lineage=real_task.kwargs['in_events'].values()[0]['metadata']['lineage'],
                                          op='recv', msg='lambda_start_ts')
                except IndexError:
                    real_task = OrphanedTask()  # task doesn't exist
                    Tracker.logger.info("get an orphaned lambda function, sending quit")

            real_task.rewire(tsk.current_state)                  # transition to a Task
            cls.fd_to_task_map[tsk.current_state.fileno()] = real_task
            tsk.current_state.update_flags()
            should_append.append(real_task)
            removable.append(tsk)
        return should_append, removable

    @classmethod
    def _profile_task_poll_loop(cls):
        # pr = cProfile.Profile()
        # pr.enable()
        cls._task_poll_loop()
        # pr.disable()
        # pr.dump_stats('tracker_prof_output.cprof')
        # Tracker.logger.info("tracker_prof_output written")
        Tracker.logger.info("Tracker: Finish main loop")

    @classmethod
    def _start(cls):
        Tracker.logger.debug("Tracker _start")
        with cls.started_lock:
            if cls.started:
                return

            cls.main_thread = threading.Thread(target=cls._profile_task_poll_loop)
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
