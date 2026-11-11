"""Run a framework CLI command with the pinned wheel on the job cluster."""

import sys

from lakeflow_framework.cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
