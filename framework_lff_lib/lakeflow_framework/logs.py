"""Log lines for the framework jobs, written to the job run's output."""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from typing import Iterator

FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_ROOT = "lakeflow_framework"


def job_logging(job: str, level: int = logging.INFO) -> logging.Logger:
    """Send every lakeflow_framework logger to stdout and return the logger for ``job``.

    Only this package's loggers are configured, so Spark and py4j keep their own levels.
    Calling it again, as a retried task in the same process does, adds no second handler.
    """

    root = logging.getLogger(_ROOT)
    root.setLevel(level)
    if not any(isinstance(handler, _Stdout) for handler in root.handlers):
        handler = _Stdout()
        handler.setFormatter(logging.Formatter(FORMAT))
        root.addHandler(handler)
    root.propagate = False
    return logging.getLogger(f"{_ROOT}.jobs.{job}")


class _Stdout(logging.StreamHandler):
    """Writes to the sys.stdout of the moment: the job runner may replace it after setup."""

    def __init__(self) -> None:
        super().__init__(sys.stdout)

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, _value) -> None:
        pass


@contextmanager
def step(log: logging.Logger, name: str) -> Iterator[None]:
    """Log the start, the end, and the seconds taken of ``name``, or the error that ended it."""

    started = time.monotonic()
    log.info("%s: started", name)
    try:
        yield
    except Exception:
        log.exception("%s: failed after %.1fs", name, time.monotonic() - started)
        raise
    log.info("%s: finished in %.1fs", name, time.monotonic() - started)


def statement_summary(statement: str, width: int = 120) -> str:
    """The statement on one line, cut to ``width`` characters, for a log line."""

    text = " ".join(statement.split())
    return text if len(text) <= width else text[: width - 3] + "..."
