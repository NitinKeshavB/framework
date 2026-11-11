# Maintaining the framework

## Versioning

The version in [framework_src/pyproject.toml](../framework_src/pyproject.toml) is the wheel version. A minor version may add an optional field to a spec. A major version may rename or remove a required one. Deploy the bundles again after a version change. Each bundle uploads the wheel it just built.

The registry `content_hash` is the config version. It is independent of the wheel version. An unchanged spec does not append a new `object_registry` row.

The registry DDL is create-only: `CREATE ... IF NOT EXISTS` for the schema, volume, and tables, and `CREATE OR REPLACE VIEW` for the views. It has no `ALTER`. Nothing has been deployed yet, so the columns are the first version. Once a catalog holds data, a column change is a migration with a plan for existing rows, and it needs its own reviewed script, not an edit to this file.

## Deploy

Order: `framework_bundle`, run `framework_ddl`, start `framework_collector`, then `framework_ingestion`, then each project.

```bash
./run.sh bundle:deploy framework_bundle <target> <profile>
./run.sh bundle:deploy framework_ingestion <target> <profile>
./run.sh bundle:deploy framework_projects/<domain>/<project> <target> <profile>
```

`bundle:validate` and `bundle:deploy` regenerate first: an ingestion deploy rewrites the pipelines, jobs, and map, then checks that every project still finds the bronze tables it reads. A project deploy regenerates the project maps. A removed or renamed bronze table that a project reads stops the ingestion deploy. That is intended.

All bundles must use the same catalog for a given target, because the ingestion jobs read the registry in the platform bundle's catalog. Check the `catalog` variable in each `databricks.yml` when you add a target.

PyPI and a wheel volume are not part of this phase. Every bundle builds `framework_src` with the same artifact block.

## Generated files

`SOURCE_MAP.md`, `pipelines/`, and `resources/` in `framework_ingestion`, and `PROJECT_MAP.md` and `PROJECTS_INDEX.md` in `framework_projects`, are generated and committed. CI should run `./run.sh generate:check` and `./run.sh projects:check`. Do not edit them by hand; the next `generate` overwrites them. A hand-written resource file must not start with the generated marker line.

## What not to change

- Do not add a writer besides the collector for `object_registry` and `run_ledger`. Jobs write JSON files only. The collector inserts a definition only when `(object_kind, full_name, content_hash)` is new, and a step only when `(run_id, step_no, status)` is new.
- Do not weaken the ownership check. The collector must keep refusing a pipeline or table another owner holds.
- Do not put `checkpoint` or `cloudFiles.schemaLocation` back into the Auto Loader options. Lakeflow owns that state for pipelines. The collector is the exception: its checkpoint is under the events volume because it is a job.
- Do not schedule a `VACUUM` that shortens retention.
- Do not port the old SQL Server `tfm_*` procedures. Load strategies and pipeline datasets replace them.
- Do not create a table or schema in a spec's name from a pipeline. Schemas and volumes are prepared outside the framework.

## Adding a source kind

A kind is a `source.yaml` rule set plus one load strategy. Add the strategy in `load.py`, name it in `LOAD_METHODS_BY_KIND` in `ingestion.py`, add a template to the scaffold, and add the graph shape in `pipeline.py` if it differs. Keep strategies free of `pyspark`: they return option dictionaries and SQL strings. Cover it with a sample source and a test that needs no Spark.

## Tests and checks

```bash
./run.sh install
./run.sh test:quick        # framework_tests and the sample consumer tests; no Spark
./run.sh lint              # pre-commit: ruff, formatting, YAML/TOML, whitespace, private keys
./run.sh generate:check
./run.sh projects:check
```

Coverage reporting is a later phase.

## Runtime checks

[END_TO_END.md](END_TO_END.md) lists the Spark and Lakeflow behaviors that still need a pass on a workspace.
