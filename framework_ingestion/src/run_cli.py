"""Run a framework CLI command with the pinned wheel on the job cluster."""

import sys

from lakeflow_framework.cli import main

if __name__ == "__main__":
    code = main(sys.argv[1:])
    # Serverless runs this file through IPython, which warns on any SystemExit, even 0.
    # Exit only on failure, so a clean run leaves a clean log and a failure still fails the task.
    if code:
        raise SystemExit(code)
