# Maintaining the framework

## Versioning

The version in [framework_lff_lib/pyproject.toml](../framework_lff_lib/pyproject.toml) is the wheel version. A minor version may add an optional field to a spec. A major version may rename or remove a required one. Deploy the bundles again after a version change. Each bundle uploads the wheel it just built.

The registry `content_hash` is the config version. It is independent of the wheel version. An unchanged spec does not append a new `object_registry` row.

The registry DDL in `framework_bundle/sql/framework_meta.sql` runs on every `framework_bundle` deploy, so each statement must be safe to run again. Tables are `CREATE ... IF NOT EXISTS` and views `CREATE OR REPLACE VIEW`. A schema change is added below the tables as a statement that is safe to repeat:

- A new column is `ALTER TABLE ... ADD COLUMNS`. The DDL task skips that statement when the column is already there.
- A new clustering key is `ALTER TABLE ... CLUSTER BY`, at most three keys.
- A backfill is an `UPDATE` whose `WHERE` matches only rows still missing the value, as `config` is filled on `object_current`. Never `UPDATE` the history tables.
- A table property change is `ALTER TABLE ... SET TBLPROPERTIES`, as `autoCompact` is turned on.
- A retired view is `DROP VIEW IF EXISTS`.

The DDL task splits the file on `;`, so a SQL comment in it, or in `system_views.sql`, must not contain a semicolon. A test enforces this. The system-table views are in `system_views.sql` and their own `framework_system_views` job, because they need `SELECT` on `system.billing` and `system.access`. The job runs them only when the target sets `system_views` to `true`, and deploy only warns if it fails. Keep everything a team needs to operate, run status included, out of that file: those grants come from a metastore admin.

## Deploy

Order: `framework_bundle` (deploy runs `framework_ddl`, then `framework_system_views`), then `framework_ingestion` (deploy runs `registry_sync`), then each project. `framework_metrics` (hourly), `framework_runs` (hourly), and `framework_optimize` (daily) are scheduled by the framework bundle. `framework_metrics` finds new `<job>_event_log` tables on its next run, so a new delivery needs no change there.

```bash
./run.sh bundle:deploy framework_bundle <target> <profile>
./run.sh bundle:deploy framework_ingestion <target> <profile>
./run.sh bundle:deploy framework_curation/<domain>/<project> <target> <profile>
```

`bundle:validate` and `bundle:deploy` regenerate first: an ingestion deploy rewrites the pipelines, jobs, and map, then checks that every project still finds the bronze tables it reads. A project deploy regenerates the project maps. A removed or renamed bronze table that a project reads stops the ingestion deploy. That is intended.

All bundles must use the same catalog for a given target, because the ingestion jobs read the registry in the platform bundle's catalog. Check the `catalog` variable in each `databricks.yml` when you add a target.

PyPI and a wheel volume are not part of this phase. Every bundle builds `framework_lff_lib` with the same artifact block. The wheel depends on PyYAML only. Databricks Connect and the DLT package are not installed into serverless environments.

## Generated files

`SOURCE_MAP.md`, `pipelines/`, and `resources/` in `framework_ingestion`, and `PROJECT_MAP.md` and `PROJECTS_INDEX.md` in `framework_curation`, are generated and committed. CI runs `./run.sh lint:ci`, `test:quick`, `generate:check`, and `projects:check`. Do not edit generated files by hand; the next `generate` overwrites them. A hand-written resource file must not start with the generated marker line.

## What not to change

- `registry_sync`, which deploy runs, is the only writer of definitions. It calls `append_definitions` in `registry.py`, which inserts `object_registry`, merges `object_current`, and records ownership refusals in `run_ledger` and `run_current`. Delivery jobs do not write the registry and must not wait on it: the pipeline reads its specs from `lff.specs`. Do not `UPDATE` or `DELETE` the history tables. Do not add another writer.
- Do not weaken the ownership check. `registry_sync` must keep refusing a pipeline or table another owner holds.
- Do not put `checkpoint` or `cloudFiles.schemaLocation` back into the Auto Loader options. Lakeflow owns that state for pipelines.
- Do not schedule a `VACUUM` that shortens retention.
- Do not create a table or schema in a spec's name from a pipeline. Schemas and volumes are prepared outside the framework.
- Add a Unity Catalog tag key only by listing it in `SCHEMA_TAGS` or `TABLE_RECORD_TAGS` in `unity.py` and in `v_metadata_drift`. `unity.py` unsets only listed keys, so tags set by others stay.
- Do not write `object_registry` except through `append_definitions` and `retire_definitions`. They keep exactly one current version per `(object_kind, full_name)`; a plain `INSERT` breaks that.
- Do not drop the `event_log` block, the `lff.specs` and `lff.catalog` settings, or the `lakeflow_framework_workflow` tag from generated pipelines and jobs. The pipeline, `table_load_metrics`, and the run views depend on them.

## Adding a source kind

A kind is a `source.yaml` rule set plus one load strategy. Add the strategy in `load.py`, name it in `LOAD_METHODS_BY_KIND` in `ingestion.py`, add a template to the scaffold, and add the graph shape in `pipeline.py` if it differs. Keep strategies free of `pyspark`: they return option dictionaries and SQL strings. Cover it with a sample source and a test that needs no Spark.

## Tests and checks

Package tests live in `framework_tests`, not inside the ingestion or curation bundles. They copy sample YAML into `tmp_path` and use fake Spark objects; they do not start a cluster.

```bash
./run.sh install
./run.sh test:quick        # framework_tests and the sample consumer tests; no Spark
./run.sh lint              # pre-commit: ruff, formatting, YAML/TOML, whitespace, private keys
./run.sh generate:check
./run.sh projects:check
```

CI runs the same four checks on every pull request (`.github/workflows/checks.yml`). Coverage reporting is a later phase.

## Runtime checks

[END_TO_END.md](END_TO_END.md) lists the Spark and Lakeflow behaviors that still need a pass on a workspace.
