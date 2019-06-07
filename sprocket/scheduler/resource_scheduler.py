#!/usr/bin/python

from sprocket.controlling.tracker.task import Task
from sprocket.controlling.tracker.tracker import Tracker
from sprocket.controlling.common.logger import get_logger
from sprocket.scheduler.abstract_schedulers import SchedulerBase
from sprocket.scheduler.resources import Resource, ResourceRequest, ResourceManager


class ResourceScheduler(SchedulerBase):

    resource_manager = ResourceManager()
    logger = get_logger(__file__.split('/')[-1])

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
                ResourceScheduler.logger.debug('Task requires: "{}"'.format(t.resources))
                resources_sufficient = True
                resources = map(ResourceRequest.from_json, t.resources)
                t.resources = resources  # TODO: instantiate resource requests somewhere else

                for resource in resources:
                    if not cls.resource_manager.resource_exists(resource.name):
                        cls.resource_manager.register_resource(resource.name, resource.max_allocation)

                    if cls.resource_manager.available_resource(resource.name) < resource.required_allocation:
                        ResourceScheduler.logger.debug("Insufficient resources for task {}. Need {} of {} but have {}".format(
                            t,
                            resource.required_allocation,
                            resource.name,
                            cls.resource_manager.available_resource(resource.name)))
                        resources_sufficient = False
                        tasks_to_redeliver.append(next_task)
                        break

                if resources_sufficient:
                    ResourceScheduler.logger.debug("Have sufficient resources for task {}".format(t))
                    cls.resource_manager.reserve_resources(resources)
                    submitted.append(t)
                    pipeline.tasks.append(t)
                    Tracker.submit(t)
                    count_submitted += 1
                    ResourceScheduler.logger.debug('submitted a task: ' + str(t))

            # if we failed to submit a task due to resource insufficiency, place it back on deliver_queue
            ResourceScheduler.logger.debug("Placing {} tasks back on delivery queue".format(len(tasks_to_redeliver)))
            map(lambda ttd: stage.deliver_queue.put(ttd), tasks_to_redeliver)
        return count_submitted

    @classmethod
    def process_finish_tasks(cls, tasks):
        for t in tasks:
            ResourceScheduler.logger.debug("Releasing resources")
            for r in t.resources:
                cls.resource_manager.release_resource(r.name, r.required_allocation)
                r.required_allocation = 0  # task is done; needs no more resources
            # t.resources = []
