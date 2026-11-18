# Maintaining the framework

## Versioning

The version in [framework_lff_lib/pyproject.toml](../framework_lff_lib/pyproject.toml) is the wheel version. A minor version may add an optional field to a spec. A major version may rename or remove a required one. Deploy the bundles again after a version change. Each bundle uploads the wheel it just built.

The registry `content_hash` is the config version. It is independent of the wheel version. An unchanged spec does not append a new `asset_registry` row.

The registry DDL in `framework_bundle/sql/framework_meta.sql` runs on a `framework_bundle` deploy whenever the file has changed, so each statement must be safe to run again. Tables are `CREATE ... IF NOT EXISTS` and views `CREATE OR REPLACE VIEW`. The same file is shipped as package data (`lakeflow_framework/sql/framework_meta.sql`) so `metadata.ddl_text()` works from a wheel, not only a repo checkout.

Until go-live the file describes a fresh schema and has no `ALTER` statements. To change a table, edit its `CREATE TABLE`, drop `<catalog>.framework_meta`, and deploy `framework_bundle` then `framework_ingestion` again; `registry_sync` writes the definitions back. Registry history, run history, and metrics start over.

After go-live, add each change below the tables as a statement that is safe to repeat, and keep the `CREATE TABLE` up to date for a fresh catalog:

- A new column is `ALTER TABLE ... ADD COLUMNS`. It has no `IF NOT EXISTS`, so `apply_ddl` must skip it when the column is there.
- A new clustering key is `ALTER TABLE ... CLUSTER BY`, at most three keys.
- A backfill is an `UPDATE` whose `WHERE` matches only rows still missing the value. Never `UPDATE` the history tables.
- A table property change is `ALTER TABLE ... SET TBLPROPERTIES`.
- A retired view is `DROP VIEW IF EXISTS`.

The DDL task splits the file on `;`, so a SQL comment in it, or in `system_views.sql`, must not contain a semicolon. A test enforces this. The system-table views are in `system_views.sql` and their own `framework_system_views` job, because they need `SELECT` on `system.billing` and `system.access`. The job runs them only when the target sets `system_views` to `true`, and deploy only warns if it fails. Keep everything a team needs to operate, run status included, out of that file: those grants come from a metastore admin.

## Deploy

Order: `framework_bundle` (deploy runs `framework_ddl`, which does nothing when `framework_meta.sql` is unchanged since its last run, then `framework_system_views`), then `framework_ingestion` (deploy runs `registry_sync`), then each project. `framework_runs` (hourly catch-up; prefers `system.lakeflow`, Jobs API fallback) and `framework_optimize` (daily: OPTIMIZE, `archive_hot`) are scheduled by the framework bundle. Each delivery job records its own load metrics (`ALL_DONE`) and NRT `job_runs` via `record_this_run`, so a new delivery needs no metrics-pipeline change. Until go-live, drop/recreate `framework_meta` when DDL adds columns (`CREATE IF NOT EXISTS` will not ALTER).

For production isolation: set `run_as: <service-principal-application-id>` in each `source.yaml`, regenerate, and grant that SP only the source's raw volume (or foreign catalog) and bronze/quarantine schemas. Quality-rule expressions are constrained, but they still run as that principal.

```bash
./run.sh bundle:deploy framework_bundle <target> <profile>
./run.sh bundle:deploy framework_ingestion <target> <profile>
./run.sh bundle:deploy framework_curation/<domain>/<project> <target> <profile>
```

`bundle:validate` and `bundle:deploy` regenerate first: an ingestion deploy rewrites the pipelines, jobs, and map, then checks that every project still finds the bronze tables it reads. A project deploy regenerates the project maps. A removed or renamed bronze table that a project reads stops the ingestion deploy. That is intended.

All bundles must use the same catalog for a given target, because the ingestion jobs read the registry in the platform bundle's catalog. Check the `catalog` variable in each `databricks.yml` when you add a target.

PyPI and a wheel volume are not part of this phase. Every bundle builds `framework_lff_lib` with the same artifact block. The wheel depends on PyYAML only. Databricks Connect and the DLT package are not installed into serverless environments.

## Generated files

Ingestion commits only `SOURCE_MAP.md`. Job/pipeline YAML, pipeline entrypoints, and per-delivery `manifests/*.json` are written under gitignored `framework_ingestion/.build/` at `generate` / `bundle:validate` / `bundle:deploy`. `generate:check` drifts the map alone so concurrent PRs do not fight over regenerated resources. Curation still commits `PROJECT_MAP.md` and `PROJECTS_INDEX.md`. CI runs `./run.sh lint:ci`, `test:quick`, `generate:check`, and `projects:check`. Do not edit generated files by hand. A hand-written resource under `resources/` must not start with the generated marker line.

## What not to change

- `registry_sync`, which deploy runs, is the only writer of definitions. It calls `append_definitions` in `registry.py`, which inserts `asset_registry`, merges `asset_registry_current`, and records ownership refusals in `ownership_refusals`. Each asset carries a stable `asset_id` (SHA-256 of source coordinates). Hash and ownership lookups **join staged temp views** (never megabyte `IN` lists). Retirement is scoped to the **owners this bundle deploys**, so ingestion can be sharded per source system without wiping another shard; each row carries `bundle_id`. Delivery jobs do not write the registry and must not wait on it: the pipeline reads its delivery from `lff.manifest` (legacy `lff.specs` still accepted). Do not `UPDATE` or `DELETE` the history tables. Do not add another writer. Pipeline event logs live in `{catalog}.framework_events`, not `framework_meta`.
- Do not weaken the ownership check. `registry_sync` must keep refusing a pipeline or table another owner holds.
- Do not put `checkpoint` or `cloudFiles.schemaLocation` back into the Auto Loader options. Lakeflow owns that state for pipelines.
- Do not schedule a `VACUUM` that shortens retention.
- Do not create a table or schema in a spec's name from a pipeline. Schemas and volumes are prepared outside the framework.
- Add a Unity Catalog tag key only by listing it in `SCHEMA_TAGS` or `TABLE_RECORD_TAGS` in `unity.py` and in `v_metadata_drift`. `unity.py` unsets only listed keys, so tags set by others stay.
- Do not write `asset_registry` except through `append_definitions` and `retire_definitions`. They keep exactly one current version per `(object_kind, full_name)`; a plain `INSERT` breaks that.
- Do not drop the `event_log` block, the `lff.manifest` / `lff.catalog` settings, or the `lakeflow_framework_workflow` tag from generated pipelines and jobs. The pipeline, `record_load_metrics` / `v_loads`, and the run views depend on them.
- Do not reintroduce retired meta objects (`run_ledger`, `run_current`, `v_process`, `v_run_current`, `v_bronze_loads`, `load_daily_rollup`, ops `flow_events` / `table_load_metrics`). See [META_MODEL_REVIEW.md](META_MODEL_REVIEW.md).

## Adding a source kind

A kind is a `source.yaml` rule set plus one load strategy. Add the strategy in `load.py`, name it in `LOAD_METHODS_BY_KIND` in `sources.py` (re-exported from `ingestion.py`), add a template in `scaffold.py`, and add the graph shape in `pipeline.py` if it differs. Keep strategies free of `pyspark`: they return option dictionaries and SQL strings. Cover it with a sample source and a test that needs no Spark.

Ingestion layout is split: `sources.py` (load/validate/registry rows), `generate.py` (DABs + SOURCE_MAP), `scaffold.py` (new-source / new-table). `ingestion.py` re-exports the public surface. Shared SQL quoting lives in `sqlnames.py`. Registry DDL ships as package data under `lakeflow_framework/sql/` (and still under `framework_bundle/sql/` for the DDL job).

## Tests and checks

Package tests live in `framework_tests`, not inside the ingestion or curation bundles. They copy sample YAML into `tmp_path` and use fake Spark objects; they do not start a cluster.

```bash
./run.sh install
./run.sh test:quick        # framework_tests and the sample consumer tests; no Spark
./run.sh lint              # pre-commit on every file (hooks may auto-fix)
./run.sh generate:check
./run.sh projects:check
```

CI runs lint (ruff E/F/I/B/S/UP), mypy (pragmatic; see `framework_lff_lib/mypy.ini`), `test:quick`, `generate:check`, and `projects:check` on every pull request. Coverage reporting is a later phase.

### Pre-commit

Config: [`.pre-commit-config.yaml`](../.pre-commit-config.yaml). Commits run hooks on **staged** files only; a hook with no matching staged files shows `(no files to check) Skipped`. `./run.sh lint` / `lint:ci` run every hook on all files.

| Group | Hooks |
| --- | --- |
| Hygiene | merge-conflict markers, case-conflict, symlinks, YAML/TOML parse, EOF newline, trailing whitespace, mixed line endings (fail only when one file mixes CRLF and LF — does not force LF), large files (&gt;500 KB), private keys, leftover `pdb` / `breakpoint()`, pytest test file naming |
| Python | Ruff lint (`E,F,I,B,S,UP` via `ruff.toml`) and Ruff format; mypy on `framework_lff_lib/lakeflow_framework/` (pragmatic `mypy.ini`) |
| Local | `./run.sh generate:check` when `framework_ingestion/` or `framework_lff_lib/` changes; `./run.sh projects:check` when `framework_curation/`, `framework_examples/ingestion/`, or `framework_lff_lib/` changes |

After changing the config, run `./run.sh lint` once so hook environments refresh.

## Runtime checks

[END_TO_END.md](END_TO_END.md) lists the Spark and Lakeflow behaviors that still need a pass on a workspace.
