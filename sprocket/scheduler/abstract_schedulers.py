#!/usr/bin/python

import logging
import time

from sprocket.controlling.common.logger import get_logger
from sprocket.config import settings
from sprocket.controlling.tracker.machine_state import ErrorState, TerminalState, WaitForInputState
from sprocket.controlling.tracker.tracker import Tracker
from sprocket.delivery_function.default_delivery_func import default_delivery_func
from sprocket.scheduler.util import print_task_states
from sprocket.util.durable_queue import DurableQueue


class SchedulerBase(object):

    logger = get_logger("SchedulerBase")
    should_stop = False

    @classmethod
    def schedule(cls, pipeline):
        SchedulerBase.logger.info('start scheduling pipeline: %s', pipeline.pipe_id)
        last_print = 0
        tasks = []
        while not cls.should_stop:
            buffer_empty = True
            deliver_empty = True
            for key, stage in pipeline.stages.iteritems():
                stage.delivery_func = default_delivery_func if stage.delivery_func is None else stage.delivery_func
                if any([not q.empty() and not isinstance(q, DurableQueue) for q in stage.buffer_queues.values()]):
                    buffer_empty = False
                    stage.delivery_func(stage.buffer_queues, stage.deliver_queue,
                                        stale=len(tasks) == 0 and stage.deliver_queue.empty(),
                                        stage_conf=stage.config, stage_context=stage.context, pipedata=pipeline.pipedata)

            submitted_task_count = cls.submit_tasks(pipeline, tasks)
            if submitted_task_count != 0:
                deliver_empty = False

            if hasattr(cls, 'get_quota'):
                if cls.get_quota(pipeline) == 0:  # if pipeline is sleeping
                    deliver_empty = False
                elif not cls.get_deliverQueueEmpty(pipeline):
                    deliver_empty = False

            deliver_empty = cls.get_deliver_queue_empty(pipeline)

            finished_tasks = [t for t in tasks if
                                isinstance(t.current_state, ErrorState) or
                                isinstance(t.current_state, TerminalState) or
                                isinstance(t.current_state, WaitForInputState)]
            error_tasks = [t for t in tasks if isinstance(t.current_state, ErrorState)]

            SchedulerBase.logger.debug("Have {} finished tasks".format(len(finished_tasks)))
            cls.process_finish_tasks(finished_tasks)
            if len(error_tasks) > 0:
                logging.error(str(len(error_tasks)) + " tasks failed: ")
                errmsgs = []
                for et in error_tasks:
                    logging.error(et.current_state.str_extra())
                    errmsgs.append(et.current_state.str_extra())
                raise Exception(str(len(error_tasks)) + " tasks failed\n" + "\n".join(errmsgs))
            tasks = [t for t in tasks if t not in finished_tasks and not isinstance(t.current_state, WaitForInputState)]

            if buffer_empty and deliver_empty and len(tasks) == 0:
                break

            if time.time() > last_print+1:
                print_task_states(tasks)
                # SchedulerBase.logger.debug("buffer empty: "+str(buffer_empty)+', deliver empty: '+str(deliver_empty))
                last_print = time.time()
            time.sleep(1)
            # sleep to avoid spinning, we can use notification instead, but so far, this works.
            # it may increase overall latency by at most n*0.01 second, where n is the longest path in the pipeline

        SchedulerBase.logger.info('finish scheduling pipeline')

    @classmethod
    def submit_tasks(cls, pipeline, submitted):
        raise NotImplementedError()

    @classmethod
    def stop(cls):
        cls.should_stop = True

    @classmethod
    def get_deliver_queue_empty(cls, pipeline):
        deliver_empty = True
        for key, stage in pipeline.stages.iteritems():
            if not stage.deliver_queue.empty():
                deliver_empty = False
                return deliver_empty
        return deliver_empty

    @classmethod
    def process_finish_tasks(cls, tasks):
        pass


class ThrottledScheduler(SchedulerBase):

    @classmethod
    def submit_tasks(cls, pipeline, submitted):
        count_submitted = 0
        quota = cls.get_quota(pipeline)
        for t in cls.task_gen(pipeline, quota):
            submitted.append(t)
            pipeline.tasks.append(t)
            Tracker.submit(t)
            count_submitted += 1
            logging.debug('submitted a task: %s', t)
        cls.consume_quota(pipeline, count_submitted)
        return count_submitted

    @classmethod
    def get_quota(cls, pipeline):
        raise NotImplementedError()

    @classmethod
    def consume_quota(cls, pipeline, n):
        raise NotImplementedError()

    @classmethod
    def task_gen(cls, pipeline, n):
        raise NotImplementedError()


class ConcurrencyLimitScheduler(ThrottledScheduler):
    concurrency_limit = settings.get('concurrency_limit', 1500)

    @classmethod
    def get_quota(cls, pipeline):
        running = [t for t in pipeline.tasks if not (isinstance(t.current_state, TerminalState) or
                                                     isinstance(t.current_state, ErrorState))]
        return cls.concurrency_limit - len(running)

    @classmethod
    def consume_quota(cls, pipeline, n):
        pass

    @classmethod
    def task_gen(cls, pipeline, n):
        raise NotImplementedError()


class RequestRateLimitScheduler(ThrottledScheduler):
    bucket_size = settings.get('rate_limit_bucket_size', 50)
    refill_rate = settings.get('rate_limit_refill_rate', 50)  # 50/s
    available_token = 0  # initially 0 token
    last_refill = 0

    @classmethod
    def refill(cls):
        cls.available_token = int(round(max(cls.available_token + (time.time() - cls.last_refill) * cls.refill_rate, cls.bucket_size)))
        cls.last_refill = time.time()

    @classmethod
    def get_quota(cls, pipeline):
        cls.refill()
        return cls.available_token

    @classmethod
    def consume_quota(cls, pipeline, n):
        cls.refill()
        consumed = max(cls.available_token, n)
        cls.available_token -= consumed
        return consumed

    @classmethod
    def task_gen(cls, pipeline, n):
        raise NotImplementedError()

