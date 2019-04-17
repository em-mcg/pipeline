#!/usr/bin/python
import logging
import threading
import json

from sprocket.controlling.tracker.task import Task
from sprocket.controlling.tracker.tracker import Tracker
from sprocket.scheduler.abstract_schedulers import SchedulerBase


class Resource():

    def __init__(self, name, max_allocation):
        self.name = name
        self.max_allocation = max_allocation
        self.available_allocation = max_allocation
        self.lock = threading.Lock()

    @staticmethod
    def from_json(js):
        #d = json.loads(js)
        return Resource(name=js['name'], max_allocation=js['max_allocation'])


class ResourceManager():

    def __init__(self):
        self.resources = {}
        self.resources_lock = threading.Lock()

    def register_resource(self, resource_name, amount):
        with self.resources_lock:
            self.resources[resource_name] = Resource(resource_name, amount)

    def available_resource(self, resource_name):
        with self.resources[resource_name].lock:
            return self.resources[resource_name].available_allocation

    def release_resource(self, resource_name, amount):
        with self.resources[resource_name].lock:
            self.resources[resource_name].available_allocation += amount

    def aquire_resource(self, resource_name, amount):
        with self.resources[resource_name].lock:
            self.resources[resource_name].available_allocation -= amount

    def resource_exists(self, resource_name):
        return resource_name in self.resources


class ResourceScheduler(SchedulerBase):

    resource_manager = ResourceManager()

    @classmethod
    def submit_tasks(cls, pipeline, submitted=[]):
        count_submitted = 0  # number of tasks submitted

        # submit all tasks for all stages in the pipeline
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                logging.debug("Creating task with init stage '{}' and config '{}'".format(stage.init_state, stage.config))
                next_task = stage.deliver_queue.get()  # take first task off deliver queue
                t = Task(
                    lambda_func=stage.lambda_function,
                    init_state=stage.init_state,
                    event=stage.event,
                    in_events=next_task,
                    emit_event=stage.emit,
                    config=stage.config,
                    pipe=pipeline.pipedata,
                    regions=stage.region,
                )
                # let scheduler, pipeline, and tracker see the created task
                logging.debug('Task requires: {}'.format(t.resources))
                resources_sufficient = True
                for resource in map(Resource.from_json, t.resources):
                    if not cls.resource_manager.resource_exists(resource.name):
                        cls.resource_manager.register_resource(resource.name, resource.max_allocation)

                    if cls.resource_manager.available_resource(resource.name) <= 0:
                        logging.debug("Insufficient resources for task {}. Need {} of {} but have {}".format(
                            t, resource.max_allocation, resource.name, cls.resource_manager.available_resource(resource.name)))
                        resources_sufficient = False
                        break

                if resources_sufficient:
                    logging.debug("Have sufficient resources for task {}".format(t))
                    cls.submitted_tasks.append(t)
                    pipeline.tasks.append(t)
                    Tracker.submit(t)
                    count_submitted += 1
                    logging.debug('submitted a task: ' + str(t))

        return count_submitted
