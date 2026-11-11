#!/bin/bash

set -euo pipefail

#####################
# --- Constants --- #
#####################

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PKG_DIR="$THIS_DIR/framework_src"
PYTHON="${PYTHON:-python3}"


##########################
# --- Task Functions --- #
##########################

# install core and development Python dependencies into the currently activated venv
function install {
    "$PYTHON" -m pip install --upgrade pip
    "$PYTHON" -m pip install --editable "$PKG_DIR[dev]"
}

# run all pre-commit hooks (ruff, ruff-format, file hygiene) on every file; hooks may fix files
function lint {
    (cd "$THIS_DIR" && "$PYTHON" -m pre_commit run --all-files)
}

# same as `lint` but for CI: shows the diff when a hook changed files
function lint:ci {
    (cd "$THIS_DIR" && "$PYTHON" -m pre_commit run --all-files --show-diff-on-failure)
}

# execute tests that are not marked as `slow`; extra args go to pytest
function test:quick {
    "$PYTHON" -m pytest -c "$PKG_DIR/pyproject.toml" -m "not slow" -q "$@"
}

# execute all tests, including the slow workspace tests; extra args go to pytest
function test:all {
    "$PYTHON" -m pytest -c "$PKG_DIR/pyproject.toml" -q "$@"
}

# build the wheel into framework_src/dist (same command the bundle artifact uses)
function build {
    rm -rf "$PKG_DIR/dist"
    (cd "$PKG_DIR" && "$PYTHON" -m pip wheel --no-deps --wheel-dir dist .)
    ls "$PKG_DIR/dist"
}

# validate the source systems and table specs, then regenerate from them:
# SOURCE_MAP.md, and one pipeline file, pipeline resource, and job per source delivery
function generate {
    "$PYTHON" -m lakeflow_framework.cli generate "$THIS_DIR/framework_ingestion"
}

# create source.yaml for a new source system:
# ./run.sh new-source <domain> <source_system> <owner-email> [kind] [delivery]
function new-source {
    "$PYTHON" -m lakeflow_framework.cli new-source "$THIS_DIR/framework_ingestion" \
        "${1:?usage: ./run.sh new-source <domain> <source_system> <owner-email> [kind] [delivery]}" \
        "${2:?source_system is required}" --owner "${3:?owner email is required}" \
        --kind "${4:-sftp_files}" --delivery "${5:-daily}"
}

# create a table spec in the right folder, from a template:
# ./run.sh new-table <domain> <source_system> <table> [requested-by] [delivery]
function new-table {
    local requested_args=()
    local delivery_args=()
    [ -n "${4:-}" ] && requested_args=(--requested-by "$4")
    [ -n "${5:-}" ] && delivery_args=(--delivery "$5")
    "$PYTHON" -m lakeflow_framework.cli new-table "$THIS_DIR/framework_ingestion" \
        "${1:?usage: ./run.sh new-table <domain> <source_system> <table> [requested-by] [delivery]}" \
        "${2:?source_system is required}" "${3:?table is required}" \
        "${requested_args[@]}" "${delivery_args[@]}"
}

# validate every project against the ingestion bundle and the other projects, then rewrite
# framework_curation/PROJECTS_INDEX.md and each project's PROJECT_MAP.md
function projects:generate {
    "$PYTHON" -m lakeflow_framework.cli projects generate "$THIS_DIR/framework_curation" \
        --sources "$THIS_DIR/framework_ingestion"
}

# fail if a project map is out of date (for CI); changes nothing
function projects:check {
    "$PYTHON" -m lakeflow_framework.cli projects generate "$THIS_DIR/framework_curation" \
        --sources "$THIS_DIR/framework_ingestion" --check
}

# create a project folder with its bundle file: ./run.sh new-project <domain> <project> <owner-email>
function new-project {
    "$PYTHON" -m lakeflow_framework.cli projects new "$THIS_DIR/framework_curation" \
        --domain "${1:?usage: ./run.sh new-project <domain> <project> <owner-email>}" \
        --project "${2:?project is required}" --owner "${3:?owner email is required}"
}

# fail if the generated files are out of date (for CI); changes nothing
function generate:check {
    "$PYTHON" -m lakeflow_framework.cli generate "$THIS_DIR/framework_ingestion" --check
}

# validate a bundle: ./run.sh bundle:validate <bundle> <target> [databricks-profile]
# <bundle> is a folder with a databricks.yml: framework_bundle, framework_ingestion, or
# framework_curation/<domain>/<project>
function bundle:validate {
    local bundle="${1:?usage: ./run.sh bundle:validate <bundle> <target> [databricks-profile]}"
    local target="${2:?usage: ./run.sh bundle:validate <bundle> <target> [databricks-profile]}"
    local profile_args=()
    [ -n "${3:-}" ] && profile_args=(--profile "$3")
    _require-bundle "$bundle"
    _prepare-bundle "$bundle"
    (cd "$THIS_DIR/$bundle" && databricks bundle validate -t "$target" "${profile_args[@]}")
}

# validate, then deploy a bundle: ./run.sh bundle:deploy <bundle> <target> [databricks-profile]
function bundle:deploy {
    local bundle="${1:?usage: ./run.sh bundle:deploy <bundle> <target> [databricks-profile]}"
    local target="${2:?usage: ./run.sh bundle:deploy <bundle> <target> [databricks-profile]}"
    local profile_args=()
    [ -n "${3:-}" ] && profile_args=(--profile "$3")
    _require-bundle "$bundle"
    _prepare-bundle "$bundle"
    (
        cd "$THIS_DIR/$bundle"
        databricks bundle validate -t "$target" "${profile_args[@]}"
        databricks bundle deploy -t "$target" "${profile_args[@]}"
    )
}

# steps that must finish before a bundle is validated or deployed, so maps match what is deployed
# ingestion: validate every spec, regenerate the map, pipelines, and jobs, then check that every
#   project still finds the bronze tables it reads
# a project (framework_curation/<domain>/<project>): validate it and regenerate the project maps
function _prepare-bundle {
    case "$1" in
        framework_ingestion)
            generate
            projects:generate
            ;;
        framework_curation/*)
            projects:generate
            ;;
    esac
}

# stop with a clear message when the folder is not a bundle
function _require-bundle {
    if [ ! -f "$THIS_DIR/$1/databricks.yml" ]; then
        echo "error: $1 has no databricks.yml (try framework_bundle, framework_ingestion, or framework_curation/<domain>/<project>)" >&2
        return 1
    fi
}

# remove built wheels, build output, and tool caches; leaves venv and sources alone
function clean {
    rm -rf "$PKG_DIR/dist" "$PKG_DIR/build" "$PKG_DIR"/*.egg-info
    find "$THIS_DIR" \
        \( -path "$THIS_DIR/venv" -o -path "$THIS_DIR/.venv" -o -path "$THIS_DIR/.git" \) -prune -o \
        -type d \( -name __pycache__ -o -name .pytest_cache -o -name .ruff_cache \) \
        -exec rm -rf {} +
    echo "cleaned"
}

# print all functions in this file
function help {
    echo "$0 <task> <args>"
    echo "Tasks:"
    compgen -A function | grep -v '^_' | cat -n
}

TIMEFORMAT="Task completed in %3lR"
time "${@:-help}"
