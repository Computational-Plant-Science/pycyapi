from os import environ
from pathlib import Path
from platform import system
from shutil import copytree

import pytest


def is_in_ci():
    # if running in GitHub Actions CI, "CI" variable always set to true
    # https://docs.github.com/en/actions/learn-github-actions/environment-variables#default-environment-variables
    return bool(environ.get("CI", None))


def requires_platform(platform, ci_only=False):
    return pytest.mark.skipif(
        system().lower() != platform.lower() and (is_in_ci() if ci_only else True),
        reason=f"only compatible with platform: {platform.lower()}",
    )


def excludes_platform(platform, ci_only=False):
    return pytest.mark.skipif(
        system().lower() == platform.lower() and (is_in_ci() if ci_only else True),
        reason=f"not compatible with platform: {platform.lower()}",
    )


@pytest.fixture(scope="function")
def function_tmpdir(tmpdir_factory, request) -> Path:
    node = request.node.name \
        .replace("/", "_") \
        .replace("\\", "_") \
        .replace(":", "_")
    temp = Path(tmpdir_factory.mktemp(node))
    yield Path(temp)

    keep = request.config.getoption("--keep")
    if keep:
        copytree(temp, Path(keep) / temp.name)

    keep_failed = request.config.getoption("--keep-failed")
    if keep_failed and request.node.rep_call.failed:
        copytree(temp, Path(keep_failed) / temp.name)


@pytest.fixture(scope="class")
def class_tmpdir(tmpdir_factory, request) -> Path:
    assert (
            request.cls is not None
    ), "Class-scoped temp dir fixture must be used on class"
    temp = Path(tmpdir_factory.mktemp(request.cls.__name__))
    yield temp

    keep = request.config.getoption("--keep")
    if keep:
        copytree(temp, Path(keep) / temp.name)


@pytest.fixture(scope="module")
def module_tmpdir(tmpdir_factory, request) -> Path:
    temp = Path(tmpdir_factory.mktemp(request.module.__name__))
    yield temp

    keep = request.config.getoption("--keep")
    if keep:
        copytree(temp, Path(keep) / temp.name)


@pytest.fixture(scope="session")
def session_tmpdir(tmpdir_factory, request) -> Path:
    temp = Path(tmpdir_factory.mktemp(request.session.name))
    yield temp

    keep = request.config.getoption("--keep")
    if keep:
        copytree(temp, Path(keep) / temp.name)


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    # this is necessary so temp dir fixtures can
    # inspect test results and check for failure
    # (see https://doc.pytest.org/en/latest/example/simple.html#making-test-result-information-available-in-fixtures)

    outcome = yield
    rep = outcome.get_result()

    # report attribute for each phase (setup, call, teardown)
    # we're only interested in result of the function call
    setattr(item, "rep_" + rep.when, rep)


def pytest_addoption(parser):
    parser.addoption(
        "-K",
        "--keep",
        action="store",
        default=None,
        help="Move the contents of temporary test directories to correspondingly named subdirectories at the given "
             "location after tests complete. This option can be used to exclude test results from automatic cleanup, "
             "e.g. for manual inspection. The provided path is created if it does not already exist. An error is "
             "thrown if any matching files already exist.",
    )

    parser.addoption(
        "--keep-failed",
        action="store",
        default=None,
        help="Move the contents of temporary test directories to correspondingly named subdirectories at the given "
             "location if the test case fails. This option automatically saves the outputs of failed tests in the "
             "given location. The path is created if it doesn't already exist. An error is thrown if files with the "
             "same names already exist in the given location.",
    )

    parser.addoption(
        "-S",
        "--smoke",
        action="store_true",
        default=False,
        help="Run only smoke tests (should complete in <1 minute)."
    )


def pytest_runtest_setup(item):
    smoke = item.config.getoption("--smoke")
    slow = list(item.iter_markers(name="slow"))
    if smoke and slow:
        pytest.skip()
