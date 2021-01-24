from enum import IntEnum


class Status(IntEnum):
    CREATED = 1
    PULLING = 2
    RUNNING = 3
    ZIPPING = 4
    PUSHING = 5
    COMPLETED = 6
    FAILED = 7
