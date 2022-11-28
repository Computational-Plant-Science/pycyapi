import pytest

from pycyapi.utils import pattern_matches


def test_pattern_patches():
    path = "/some/PATH/with/LOWER/and/UPPER/cAsE"

    assert pattern_matches(path, ["some"])
    assert pattern_matches(path, ["case"])
    assert pattern_matches(path, ["lower"])
    assert pattern_matches(path, ["LOWER"])
    assert pattern_matches(path, ["upper"])
    assert not pattern_matches(path, ["nomatch"])


@pytest.mark.skip("todo")
def test_list_local_files():
    pass
