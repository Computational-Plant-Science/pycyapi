import logging
import traceback
from warnings import warn

import requests
from requests import HTTPError, ReadTimeout, RequestException, Timeout
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__file__)


def parse_image_components(value):
    v = (
        value.split("#", 1)[0].strip()
    )  # get rid of comments first

    if "/" in v:
        splt = v.split("/")
        name = splt[-1]
        owner = splt[-2]
    else:
        name = v
        owner = None

    if ":" in name:
        name_splt = name.split(":")
        name = name_splt[0]
        tag = name_splt[1]
    else:
        tag = None

    return owner, name, tag


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(
        retry_if_exception_type(ConnectionError)
        | retry_if_exception_type(RequestException)
        | retry_if_exception_type(ReadTimeout)
        | retry_if_exception_type(Timeout)
        | retry_if_exception_type(HTTPError)
    ),
)
def image_exists(name, owner=None, tag=None):
    url = f"https://hub.docker.com/v2/repositories/{owner if owner is not None else 'library'}/{name}/"
    if tag is not None:
        url += f"tags/{tag}/"
    response = requests.get(url)
    try:
        content = response.json()
        if "user" not in content and "name" not in content:
            return False
        if (
            content["name"] != tag
            and content["name"] != name
            and content["user"] != (owner if owner is not None else "library")
        ):
            return False
        return True
    except:
        warn(traceback.format_exc())
        return False
