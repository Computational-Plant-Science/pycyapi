import re
from os import listdir
from os.path import isfile, join


def pattern_matches(path, patterns):
    return any(pattern.lower() in path.lower() for pattern in patterns)


def list_local_files(
    path,
    include_patterns=None,
    include_names=None,
    exclude_patterns=None,
    exclude_names=None,
):
    # gather all files
    all_paths = [
        join(path, file) for file in listdir(path) if isfile(join(path, file))
    ]

    # add files matching included patterns
    included_by_pattern = (
        [
            pth
            for pth in all_paths
            if any(
                pattern.lower() in pth.lower() for pattern in include_patterns
            )
        ]
        if include_patterns is not None
        else all_paths
    )

    # add files included by name
    included_by_name = (
        (
            [
                pth
                for pth in all_paths
                if pth.rpartition("/")[2] in [name for name in include_names]
            ]
            if include_names is not None
            else included_by_pattern
        )
        + [pth for pth in all_paths if pth in [name for name in include_names]]
        if include_names is not None
        else included_by_pattern
    )

    # gather only included files
    included = set(included_by_pattern + included_by_name)

    # remove files matched excluded patterns
    excluded_by_pattern = (
        [
            name
            for name in included
            if all(
                pattern.lower() not in name.lower()
                for pattern in exclude_patterns
            )
        ]
        if exclude_patterns is not None
        else included
    )

    # remove files excluded by name
    excluded_by_name = (
        [
            pattern
            for pattern in excluded_by_pattern
            if pattern.split("/")[-1] not in exclude_names
        ]
        if exclude_names is not None
        else excluded_by_pattern
    )

    return excluded_by_name


def del_none(d) -> dict:
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.

    Referenced from https://stackoverflow.com/a/4256027.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d  # For convenience


BYTE_SYMBOLS = {
    "customary": ("B", "K", "M", "G", "T", "P", "E", "Z", "Y"),
    "customary_ext": (
        "byte",
        "kilo",
        "mega",
        "giga",
        "tera",
        "peta",
        "exa",
        "zetta",
        "iotta",
    ),
    "iec": ("Bi", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"),
    "iec_ext": (
        "byte",
        "kibi",
        "mebi",
        "gibi",
        "tebi",
        "pebi",
        "exbi",
        "zebi",
        "yobi",
    ),
}


def readable_bytes(n, format="%(value).1f %(symbol)s", symbols="customary"):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec", or "iec_ext".

    Referenced from https://stackoverflow.com/a/13449587.

      >>> readable_bytes(0)
      '0.0 B'
      >>> readable_bytes(0.9)
      '0.0 B'
      >>> readable_bytes(1)
      '1.0 B'
      >>> readable_bytes(1.9)
      '1.0 B'
      >>> readable_bytes(1024)
      '1.0 K'
      >>> readable_bytes(1048576)
      '1.0 M'
      >>> readable_bytes(1099511627776127398123789121)
      '909.5 Y'

      >>> readable_bytes(9856, symbols="customary")
      '9.6 K'
      >>> readable_bytes(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> readable_bytes(9856, symbols="iec")
      '9.6 Ki'
      >>> readable_bytes(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> readable_bytes(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> readable_bytes(10000, format="%(value).5f %(symbol)s")
      '9.76562 K'
    """

    n = int(n)
    if n < 0:
        raise ValueError("n < 0")

    symbols = BYTE_SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10

    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()

    return format % dict(symbol=symbols[0], value=n)


# referenced from https://stackoverflow.com/a/21857132/6514033
def replace_text(path, pattern, replace, flags=0):
    with open(path, "r+") as file:
        contents = file.read()
        contents = re.compile(re.escape(pattern), flags).sub(replace, contents)
        file.seek(0)
        file.truncate()
        file.write(contents)


# istarmap.py for Python <3.8
import multiprocessing.pool as mpp


def istarmap(self, func, iterable, chunksize=1):
    """starmap-version of imap"""
    if self._state != mpp.RUN:
        raise ValueError("Pool not running")

    if chunksize < 1:
        raise ValueError("Chunksize must be 1+, not {0:n}".format(chunksize))

    task_batches = mpp.Pool._get_tasks(func, iterable, chunksize)
    result = mpp.IMapIterator(self._cache)
    self._taskqueue.put(
        (
            self._guarded_task_generation(
                result._job, mpp.starmapstar, task_batches
            ),
            result._set_length,
        )
    )
    return (item for chunk in result for item in chunk)


mpp.Pool.istarmap = istarmap
