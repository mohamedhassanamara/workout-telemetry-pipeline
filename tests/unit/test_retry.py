import pytest

from telemetry.retry import retry


def flaky(failures: int):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] <= failures:
            raise ConnectionError("boom")
        return "ok"

    return fn, calls


def test_succeeds_after_transient_failures():
    fn, calls = flaky(2)
    assert retry(fn, retry_on=(ConnectionError,), sleep=lambda s: None) == "ok"
    assert calls["n"] == 3


def test_gives_up_after_max_attempts():
    fn, calls = flaky(10)
    with pytest.raises(ConnectionError):
        retry(fn, retry_on=(ConnectionError,), attempts=4, sleep=lambda s: None)
    assert calls["n"] == 4


def test_does_not_retry_other_errors():
    def fn():
        raise ValueError("bad data")

    with pytest.raises(ValueError):
        retry(fn, retry_on=(ConnectionError,), sleep=lambda s: None)
