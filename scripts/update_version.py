import argparse
import textwrap
from os import PathLike
from pathlib import Path
from typing import NamedTuple

from filelock import FileLock

_project_name = "pycyapi"
_project_root_path = Path(__file__).parent.parent
_version_file_path = _project_root_path / "version.txt"


class Version(NamedTuple):
    """Semantic version number"""

    major: int = 0
    minor: int = 0
    patch: int = 0

    def __repr__(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    @classmethod
    def from_string(cls, version: str) -> "Version":
        t = version.split(".")

        vmajor = int(t[0])
        vminor = int(t[1])
        vpatch = int(t[2])

        return cls(major=vmajor, minor=vminor, patch=vpatch)

    @classmethod
    def from_file(cls, path: PathLike) -> "Version":
        path = Path(path).expanduser().absolute()
        lines = [line.rstrip("\n") for line in open(Path(path), "r")]

        vmajor = vminor = vpatch = None
        for line in lines:
            line = line.strip()
            if not any(line):
                continue
            t = line.split('.')
            vmajor = int(t[0])
            vminor = int(t[1])
            vpatch = int(t[2])

        assert vmajor is not None and vminor is not None and vpatch is not None, "version string must follow semantic version format: major.minor.patch"

        return cls(major=vmajor, minor=vminor, patch=vpatch)


_initial_version = Version(0, 0, 1)
_current_version = Version.from_file(_version_file_path)


def update_version_txt(version: Version):
    with open(_version_file_path, "w") as f:
        f.write(str(version))
    print(f"Updated {_version_file_path} to version {version}")


def update_version(version: Version = None):
    lock_path = Path(_version_file_path.name + ".lock")
    try:
        lock = FileLock(lock_path)
        previous = Version.from_file(_version_file_path)
        version = (
            version
            if version
            else Version(previous.major, previous.minor, previous.patch)
        )

        with lock:
            update_version_txt(version)
    finally:
        lock_path.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=f"Update {_project_name} version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Update version information stored in version.txt in the project root,
            as well as several other files in the repository. If --version is not
            provided, the version number will not be changed. A file lock is held
            to synchronize file access. The version tag must comply with standard
            '<major>.<minor>.<patch>' format conventions for semantic versioning.
            """
        )
    )
    parser.add_argument(
        "-v",
        "--version",
        required=False,
        help="Specify the release version",
    )
    parser.add_argument(
        "-g",
        "--get",
        required=False,
        action="store_true",
        help="Just get the current version number, don't update anything (defaults to false)",
    )
    args = parser.parse_args()

    if args.get:
        print(Version.from_file(_project_root_path / "version.txt"))
    else:
        update_version(version=Version.from_string(args.version) if args.version else _current_version)
