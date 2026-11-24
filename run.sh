#!/bin/bash

set -euo pipefail

#####################
# --- Constants --- #
#####################

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PKG_DIR="$THIS_DIR/framework_lff_lib"
# Prefer the repo venv when PYTHON is unset, so ./run.sh generate works without activate.
if [ -z "${PYTHON:-}" ]; then
    if [ -x "$THIS_DIR/venv/bin/python" ]; then
        PYTHON="$THIS_DIR/venv/bin/python"
    elif [ -x "$THIS_DIR/.venv/bin/python" ]; then
        PYTHON="$THIS_DIR/.venv/bin/python"
    else
        PYTHON="python3"
    fi
fi


##########################
# --- Task Functions --- #
##########################

# install core and development Python dependencies into the currently activated venv
function install {
    "$PYTHON" -m pip install --upgrade pip
    "$PYTHON" -m pip install --editable "$PKG_DIR[dev]"
}

# fail fast with a clear fix when the editable package is missing from PYTHON
function _require-package {
    if ! "$PYTHON" -c "import lakeflow_framework" >/dev/null 2>&1; then
        echo "No module named 'lakeflow_framework' for: $PYTHON" >&2
        echo "Fix: source venv/bin/activate && ./run.sh install" >&2
        echo "Or:  PYTHON=$THIS_DIR/venv/bin/python ./run.sh install" >&2
        exit 1
    fi
}

# run all pre-commit hooks on every file (see .pre-commit-config.yaml); hooks may fix files
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

# build the wheel into framework_lff_lib/dist (same command the bundle artifact uses)
function build {
    rm -rf "$PKG_DIR/dist"
    (cd "$PKG_DIR" && "$PYTHON" -m pip wheel --no-deps --wheel-dir dist .)
    ls "$PKG_DIR/dist"
}

# validate the source systems and table specs, then regenerate from them:
# SOURCE_MAP.md (committed) plus .build/ pipelines, resources, and manifests per delivery
function generate {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli generate "$THIS_DIR/framework_ingestion"
}

# write out the Python and SQL a table's pipeline builds, to read or review (not deployed):
#   ./run.sh expand <spec.yaml> <catalog> [out-folder]
# The default folder is framework_ingestion/.build/expanded, which git ignores.
function expand {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli expand \
        "${1:?usage: ./run.sh expand <spec.yaml> <catalog> [out-folder]}" \
        --catalog "${2:?catalog is required}" \
        --out "${3:-$THIS_DIR/framework_ingestion/.build/expanded}"
}

# create source.yaml for a new source system:
# ./run.sh new-source <domain> <source_system> <owner-email> [kind] [delivery]
function new-source {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli new-source "$THIS_DIR/framework_ingestion" \
        "${1:?usage: ./run.sh new-source <domain> <source_system> <owner-email> [kind] [delivery]}" \
        "${2:?source_system is required}" --owner "${3:?owner email is required}" \
        --kind "${4:-sftp_files}" --delivery "${5:-daily}"
}

# create a table spec in the right folder, from a template:
# ./run.sh new-table <domain> <source_system> <table> [requested-by] [delivery]
function new-table {
    _require-package
    local requested_args=()
    local delivery_args=()
    [ -n "${4:-}" ] && requested_args=(--requested-by "$4")
    [ -n "${5:-}" ] && delivery_args=(--delivery "$5")
    "$PYTHON" -m lakeflow_framework.cli new-table "$THIS_DIR/framework_ingestion" \
        "${1:?usage: ./run.sh new-table <domain> <source_system> <table> [requested-by] [delivery]}" \
        "${2:?source_system is required}" "${3:?table is required}" \
        "${requested_args[@]}" "${delivery_args[@]}"
}

# validate every project against the example bronze sources and the other projects, then rewrite
# framework_curation/PROJECTS_INDEX.md and each project's PROJECT_MAP.md
function projects:generate {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli projects generate "$THIS_DIR/framework_curation" \
        --sources "$THIS_DIR/framework_examples/ingestion"
}

# fail if a project map is out of date (for CI); changes nothing
function projects:check {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli projects generate "$THIS_DIR/framework_curation" \
        --sources "$THIS_DIR/framework_examples/ingestion" --check
}

# create a project folder with its bundle file: ./run.sh new-project <domain> <project> <owner-email>
function new-project {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli projects new "$THIS_DIR/framework_curation" \
        --domain "${1:?usage: ./run.sh new-project <domain> <project> <owner-email>}" \
        --project "${2:?project is required}" --owner "${3:?owner email is required}"
}

# fail if the generated files are out of date (for CI); changes nothing
function generate:check {
    _require-package
    "$PYTHON" -m lakeflow_framework.cli generate "$THIS_DIR/framework_ingestion" --check
}

# run every quality rule of every spec on a SQL warehouse, before deploying. generate only reads a
# rule as text; this catches a regex that does not compile, a wrong argument count, and the like.
# With a catalog it also counts the rows each rule would send to quarantine in that catalog's tables.
#   ./run.sh check:rules <databricks-profile> <warehouse-id> [catalog]
function check:rules {
    local profile="${1:?usage: ./run.sh check:rules <databricks-profile> <warehouse-id> [catalog]}"
    local warehouse="${2:?usage: ./run.sh check:rules <databricks-profile> <warehouse-id> [catalog]}"
    local catalog_args=()
    [ -n "${3:-}" ] && catalog_args=(--catalog "$3")
    _require-package
    "$PYTHON" -m lakeflow_framework.cli check-rules "$THIS_DIR/framework_ingestion" \
        --profile "$profile" --warehouse "$warehouse" "${catalog_args[@]}"
}

# reload files of one table, or refresh it, through the table's own rules (the framework_backfill
# job; it needs framework_bundle deployed). Every option is KEY=VALUE after the target:
#   ./run.sh backfill <target> <databricks-profile> table=abs_gl mode=files files=a.csv,b.csv
#   ./run.sh backfill <target> <databricks-profile> table=abs_gl mode=dates date_from=2026-11-01 date_to=2026-11-03
#   ./run.sh backfill <target> <databricks-profile> table=abs_gl mode=failed
#   ./run.sh backfill <target> <databricks-profile> table=abs_gl mode=full_refresh confirm=true
# It lists the files and changes nothing unless dry_run=false is given. With no files or dates it
# reloads every file, which needs confirm=true.
function backfill {
    local target="${1:?usage: ./run.sh backfill <target> <databricks-profile> table=<id> mode=<mode> [KEY=VALUE ...]}"
    local profile="${2:?usage: ./run.sh backfill <target> <databricks-profile> table=<id> mode=<mode> [KEY=VALUE ...]}"
    shift 2
    local params=""
    # --params splits on commas, so each pair is quoted: files=a.csv,b.csv stays one value.
    for pair in "$@"; do
        params="${params:+$params,}\"$pair\""
    done
    [ -n "$params" ] || { echo "give at least table=<id> and mode=<mode>" >&2; exit 1; }
    _require-bundle framework_bundle
    (cd "$THIS_DIR/framework_bundle" && databricks bundle run framework_backfill -t "$target" --profile "$profile" --params "$params")
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
        # The workspace copy and the registry match when this returns.
        # framework_bundle applies the DDL. framework_ingestion registers every spec.
        case "$bundle" in
            framework_bundle)
                databricks bundle run framework_ddl -t "$target" "${profile_args[@]}"
                databricks bundle run framework_system_views -t "$target" "${profile_args[@]}" ||
                    echo "warning: framework_system_views failed; grant SELECT on system.billing and system.access, or set system_views to false for this target" >&2
                ;;
            framework_ingestion)
                databricks bundle run registry_sync -t "$target" "${profile_args[@]}"
                ;;
        esac
    )
}

# steps that must finish before a bundle is validated or deployed, so maps match what is deployed
# ingestion: validate every spec, regenerate the map, pipelines, and jobs. Curation is a separate
#   bundle: deploying ingestion neither reads nor checks it (run projects:generate for that)
# a project (framework_curation/<domain>/<project>): validate it and regenerate the project maps
function _prepare-bundle {
    case "$1" in
        framework_ingestion)
            generate
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
