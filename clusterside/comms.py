"""
    Communication with the server
"""
import time
import json
import requests

class Comms:
    #Possible Status States
    OK = 3
    WARN = 4
    FAILED = 2

    def update_status(self,status,description):
        pass

    def task_complete(self, task_pk):
        pass

class STDOUTComms(Comms):

    def status_str(self,status):
        if status == self.OK:
            return "OK"
        elif status == self.WARN:
            return "WARNING"
        elif status == self.FAILED:
            return "FAILED"

    def update_status(self,status,description):
        print("Status (%s): %s"%(self.status_str(status),description))

    def task_complete(self, task_pk):
        print("Task %s complete"%(task_pk))

class RESTComms(Comms):
    """
        Handles communication with the web server via calls to the REST API
    """

    def __init__(self, url, job_pk, headers=None):
        self.url = url + "jobs/%d/"%(job_pk,)

        if headers is None:
            self.headers = {}
        else:
            self.headers = headers
        self.headers["Content-Type"] = "application/json"

    def update_status(self, status, description):
        """
            Update job status.

            Args:
                status (Comms.STATES): Job state after this update
                url (str): url for server REST API
                description (str): Status description string. Descriptions
                    greater than 150 characters are truncated to
                    last 150 characters
        """
        if len(description) > 150:
            description = description[-150:] + "..."

        patch = json.dumps({
            "status_set": [
                {
                    "state": status,
                    "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "description": description
                }
            ]
            })

        response = requests.patch(self.url,
                                  patch,
                                  headers=self.headers)

        response.raise_for_status()


    def task_complete(self, task_pk):
        """
            Mark job task as complete.

            Args:
                task_pk (int): pk of task to mark as complete
        """
        patch = json.dumps({
            "task_set": [
                {
                    "pk": task_pk,
                    "complete": "true"
                }
            ]
        })

        response = requests.patch(self.url,
                                  patch,
                                  headers=self.headers)

        response.raise_for_status()
