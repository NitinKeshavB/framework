#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"

function install {
    "$PYTHON" -m pip install --upgrade pip
    "$PYTHON" -m pip install --editable "$ROOT/framework_src[dev]"
}

function lint {
    "$PYTHON" -m ruff check --config "$ROOT/ruff.toml" "$ROOT"
    "$PYTHON" -m ruff format --check --config "$ROOT/ruff.toml" "$ROOT"
}

function test:quick {
    "$PYTHON" -m pytest -c "$ROOT/framework_src/pyproject.toml" -q
}

function help {
    echo "$0 <install|lint|test:quick>"
}

"${@:-help}"
