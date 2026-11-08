# Maintaining the framework

## Versioning

The version in [framework_src/pyproject.toml](../framework_src/pyproject.toml) is the wheel version. A minor version may add an optional YAML field. A major version may rename or remove a required field. Deploy the bundle again after a version change. The bundle uploads the wheel it just built.

The registry `content_hash` is the config version. It is independent of the wheel version. The same document does not append a new `object_registry` row.

## Deploy

`databricks bundle deploy` on [framework_bundle](../framework_bundle) builds the wheel from `framework_src` and uploads it. The jobs use `framework_src/dist/*.whl`, which the bundle rewrites to that uploaded file. Run the `framework_ddl` job, then start `framework_collector`. `framework_optimize` runs daily. It compacts files. It does not delete process history. Do not add a `VACUUM` schedule that shortens retention.

A consumer bundle in this repo declares the same artifact. PyPI and a wheel volume are not part of this phase.

## What not to change

Do not add a writer besides the collector for `object_registry` and `run_ledger`. Consumer jobs write JSON files only. The collector inserts definition files and step files, and it skips a row whose `(object_kind, full_name, content_hash)` or `(run_id, step_no, status)` is already stored.

Do not put `checkpoint` or `cloudFiles.schemaLocation` back into the Auto Loader options. Lakeflow owns that state for the file pipelines. The collector is the exception: its Auto Loader checkpoint is under the events volume because it is a job, not a declarative table.

Do not port the old SQL Server `tfm_*` procedures. Load methods and pipeline datasets replace them.

## Tests

From the repository root:

```bash
./run.sh install
./run.sh test:quick
./run.sh lint
```

`test:quick` runs `framework_tests` and the sample bronze tests under `framework_examples/consumer/tests`. Neither starts Spark. Ruff formats and lints. The pre-commit config runs the same Ruff checks plus YAML, TOML, whitespace, and private-key checks. Coverage is a later phase. The wheel is built by the bundle artifact on deploy.

## Runtime checks

[END_TO_END.md](END_TO_END.md) lists the Spark and Lakeflow behaviors that still need a pass on a workspace. If one of those fails, record the observed syntax in that list and adjust the single strategy or DDL statement. Do not spread a workaround through the consumer bundles.
