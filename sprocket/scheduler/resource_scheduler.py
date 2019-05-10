#!/usr/bin/python
import logging
import threading
import json

from sprocket.controlling.tracker.task import Task
from sprocket.controlling.tracker.tracker import Tracker
from sprocket.scheduler.abstract_schedulers import SchedulerBase


# TODO: Move Resource classes somewhere else
class Resource:
    """
    Represents a simple resource
    """
    def __init__(self, name, max_allocation):
        self.name = name
        self.max_allocation = max_allocation
        self.available_allocation = max_allocation
        self.lock = threading.Lock()

    @staticmethod
    def from_json(js):
        """
        Create a Resource from a dictionary
        :param js: a python dictionary or json string
        :return: Resource object
        """
        if not isinstance(js, dict):
            js = json.loads(js)

        return Resource(
            name=js['name'],
            max_allocation=js['max_allocation']
        )


class ResourceRequest(Resource):
    """
    Represents a request for a particular resource
    """
    def __init__(self, name, max_allocation, required_allocation):
        Resource.__init__(self, name, max_allocation)
        self.required_allocation = required_allocation

    @staticmethod
    def from_json(js):
        if not isinstance(js, dict):
            js = json.loads(js)

        return ResourceRequest(
            name=js['name'],
            max_allocation=js['max_allocation'],
            required_allocation=js['required_allocation']
        )


class ResourceManager:
    """
    Manage the current resources in memory
    """

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

    def reserve_resources(self, resource_request_array):
        for r in resource_request_array:
            self.aquire_resource(r.name, r.required_allocation)

    def release_resources(self, resource_request_array):
        for r in resource_request_array:
            self.release_resource(r.name, r.required_allocation)


class ResourceScheduler(SchedulerBase):

    resource_manager = ResourceManager()

    @classmethod
    def submit_tasks(cls, pipeline, submitted=[]):
        count_submitted = 0      # number of tasks submitted
        tasks_to_redeliver = []  # tasks we failed to submit and must redeliver

        # submit all tasks for all stages in the pipeline
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
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
                logging.debug('Task requires: "{}"'.format(t.resources))
                resources_sufficient = True
                resources = map(ResourceRequest.from_json, t.resources)

                for resource in resources:
                    if not cls.resource_manager.resource_exists(resource.name):
                        cls.resource_manager.register_resource(resource.name, resource.max_allocation)

                    if cls.resource_manager.available_resource(resource.name) < resource.required_allocation:
                        logging.debug("Insufficient resources for task {}. Need {} of {} but have {}".format(
                            t,
                            resource.required_allocation,
                            resource.name,
                            cls.resource_manager.available_resource(resource.name)))
                        resources_sufficient = False
                        tasks_to_redeliver.append(next_task)
                        break

                if resources_sufficient:
                    logging.debug("Have sufficient resources for task {}".format(t))
                    cls.resource_manager.reserve_resources(resources)
                    submitted.append(t)
                    pipeline.tasks.append(t)
                    Tracker.submit(t)
                    count_submitted += 1
                    logging.debug('submitted a task: ' + str(t))

            # if we failed to submit a task due to resource insufficiency, place it back on deliver_queue
            map(lambda ttd: stage.deliver_queue.put(ttd), tasks_to_redeliver)
        return count_submitted

    @classmethod
    def process_finish_tasks(cls, tasks):
        for t in tasks:
            logging.debug("Releasing resources")
            for r in map(Resource.from_json, t.resources):
                cls.resource_manager.release_resource(r.name, r.max_allocation)

