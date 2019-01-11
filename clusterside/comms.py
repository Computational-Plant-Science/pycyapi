"""
    Communication with the server
"""
import time
import json
import requests


class Comms:
    """
        Handles communication with the web server via calls to the REST API
    """

    #Possible Status States
    OK = 3
    WARN = 4
    FAILED = 2

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
                description (str): Status description string
        """
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
