import time
import json
import requests


class Comms:
    OK = 3
    WARN = 4
    FAILED = 2

    def update_status(self, pk: str, token: str, status: str, description: str):
        pass

    def update_job(self, pk: str, token: str,  props: dict):
        pass


class STDOUTComms(Comms):
    def status_str(self, status):
        if status == self.OK:
            return "OK"
        elif status == self.WARN:
            return "WARNING"
        elif status == self.FAILED:
            return "FAILED"

    def update_status(self, pk: str, token: str,  status: str, description: str):
        print(f"Status {self.status_str(status)}: {description}")

    def update_job(self, pk: str, token: str,  props: dict):
        print(f"Job updated with: {props}")


class RESTComms(Comms):
    def __init__(self, url: str, headers: dict = None):
        """
            Args:
                url (str): The base url for the PlantIT web API
                    (<hostname>/apis/v1/ in the default Plant IT configuration)
                headers (dict): Headers to include in requests against the PlantIT web API.
                    see `[headers]<https://2.python-requests.org/en/master/user/quickstart/#custom-headers>`_
                    for details.
        """
        self.url = url
        if headers is None:
            self.headers = {}
        else:
            self.headers = headers
        self.headers["Content-Type"] = "application/json"

    def update_job(self, id: str, token: str, props: dict):
        patch = json.dumps(props)
        headers = dict(self.headers)
        headers["Authorization"] = f"Token {token}"
        response = requests.patch(self.url + f"jobs/{id}/", patch, headers=self.headers)
        response.raise_for_status()

    def update_status(self, pk, token: str,  status: str, description: str):
        if len(description) > 900:
            description = description[-900:] + "..."

        self.update_job(
            pk,
            token,
            {
                "status_set": [
                    {
                        "state": status,
                        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        "description": description
                    }
                ]
            })
