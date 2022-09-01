# job script header template

SLURM_TEMPLATE = """
#!/bin/bash
"""


# job states

SLURM_RUNNING_STATES = [
    "CF",
    "CONFIGURING",
    "PD",
    "PENDING",
    "R",
    "RUNNING",
    "RD",
    "RESV_DEL_HOLD",
    "RF",
    "REQUEUE_FED",
    "RH",
    "REQUEUE_HOLD",
    "RQ",
    "REQUEUED",
    "RS",
    "RESIZING",
    "SI",
    "SIGNALING",
    "SO",
    "STAGE_OUT",
    "S",
    "SUSPENDED",
    "ST",
    "STOPPED",
]

SLURM_SUCCESS_STATES = [
    "CG",
    "COMPLETING",
    "CD",
    "COMPLETED",
]

SLURM_CANCELLED_STATES = ["CA", "CANCELLED", "RV", "REVOKED"]

SLURM_TIMEOUT_STATES = ["DL", "DEADLINE", "TO", "TIMEOUT"]

SLURM_FAILURE_STATES = [
    "BF",
    "BOOT_FAIL",
    "F",
    "FAILED",
    "NF",
    "NODE_FAIL",
    "OOM",
    "OUT_OF_MEMORY",
    "PR",
    "PREEMPTED",
]


def is_success(status):
    return status in SLURM_SUCCESS_STATES


def is_failure(status):
    return status in SLURM_FAILURE_STATES


def is_timeout(status):
    return status in SLURM_TIMEOUT_STATES


def is_cancelled(status):
    return status in SLURM_CANCELLED_STATES


def is_complete(status):
    return (
        is_success(status)
        or is_failure(status)
        or is_timeout(status)
        or is_cancelled(status)
    )
