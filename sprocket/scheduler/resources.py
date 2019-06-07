import threading
import json

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

    def __str__(self):
        return "name: {}, max_allocation: {}, available_allocation: {}".format(
            self.name, self.max_allocation, self.available_allocation)


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

    def __str__(self):
        return "name: {}, max_allocation: {}, available_allocation: {}, required_allocation: {}".format(
            self.name, self.max_allocation, self.available_allocation, self.required_allocation)


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

