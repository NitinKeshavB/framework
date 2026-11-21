# Maintaining the framework

## Versioning

The version in [framework_lff_lib/pyproject.toml](../framework_lff_lib/pyproject.toml) is the wheel version. A minor version may add an optional field to a spec. A major version may rename or remove a required one. Deploy the bundles again after a version change. Each bundle uploads the wheel it just built.

The registry `content_hash` is the config version. It is independent of the wheel version. An unchanged spec does not append a new `asset_registry` row.

The registry DDL in `framework_bundle/sql/framework_meta.sql` runs on a `framework_bundle` deploy whenever the file has changed, so each statement must be safe to run again. Tables are `CREATE ... IF NOT EXISTS` and views `CREATE OR REPLACE VIEW`. The same file is shipped as package data (`lakeflow_framework/sql/framework_meta.sql`) so `metadata.ddl_text()` works from a wheel, not only a repo checkout.

`framework_meta.sql` is first-deploy DDL: `CREATE TABLE IF NOT EXISTS` with inline PRIMARY KEY / CHECK constraints, seed `INSERT`s for `ref_*`, and `CREATE OR REPLACE VIEW`. There are no `ALTER TABLE … ADD COLUMN` or `DROP/ADD CONSTRAINT` migration pairs. After wiping `framework_meta` (and `framework_events` if needed), deploy this file as-is.

Keep `framework_bundle/sql/framework_meta.sql` and `framework_lff_lib/lakeflow_framework/sql/framework_meta.sql` identical. Edit shapes only in `CREATE TABLE`. When a hot table and its `*_archive` twin share columns, keep the same order through `archived_at` / `archive_quarter` so `archive_hot` can copy `h.*`.

Drop `<catalog>.framework_meta` (and recreate via deploy) when the CREATE shape changes; `registry_sync` writes definitions back. Registry history, run history, and recon start over.

After go-live, if you must evolve a live catalog without a wipe, add repeatable statements **below** the CREATEs and still keep CREATE up to date for a fresh catalog:

- A new column is `ALTER TABLE ... ADD COLUMNS` (no `IF NOT EXISTS`; `apply_ddl` skips when the column exists).
- A new clustering key is `ALTER TABLE ... CLUSTER BY`, at most four keys on liquid-clustered tables.
- A backfill is an `UPDATE` whose `WHERE` matches only rows still missing the value. Never `UPDATE` the history tables.
- A table property change is `ALTER TABLE ... SET TBLPROPERTIES`.
- A retired view is `DROP VIEW IF EXISTS`.

The DDL task splits the file on `;`, so a SQL comment in it, or in `system_views.sql`, must not contain a semicolon. A test enforces this. The system-table views are in `system_views.sql` and their own `framework_system_views` job, because they need `SELECT` on `system.billing` and `system.access`. The job runs them only when the target sets `system_views` to `true`, and deploy only warns if it fails. Keep everything a team needs to operate, run status included, out of that file: those grants come from a metastore admin.

## Deploy

Order: `framework_bundle` (deploy runs `framework_ddl`, which does nothing when `framework_meta.sql` is unchanged since its last run, then `framework_system_views`), then `framework_ingestion` (deploy runs `registry_sync`), then each project. `framework_runs` (hourly: job runs from `system.lakeflow` with a Jobs API fallback, pipeline updates, and the `load_ledger` rows of updates whose own hook was cut off) and `framework_optimize` (daily: OPTIMIZE, `archive_hot`) are scheduled by the framework bundle. `framework_backfill` is not scheduled: run it with `./run.sh backfill`. Each delivery pipeline records its own loads with an event hook, so a new delivery needs no metrics-pipeline change.

**Bump `PACKAGE_VERSION` (`sources.py`, `pyproject.toml`, and the pin in `test_hygiene.py`) on every deploy that changes code.** A job's serverless environment can serve a wheel of the same version from cache, so a deploy without a bump may run old code. `run.sh` must stay executable (`chmod +x run.sh`); an editor that rewrites it can drop the bit.

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
- Do not put `checkpoint` or `cloudFiles.schemaLocation` back into the Auto Loader options of a pipeline. Lakeflow owns that state for pipelines. (`framework_backfill` is a job, not a pipeline, so it gives its own, under the `backfill_state` volume.)
- Do not turn the pipeline's own update restart back on (`pipelines.numUpdateRetryAttempts` is `0` in `generate.py`). A file blocked by a `fail` rule fails the update on purpose and is already marked read, so a restart finishes and hides the failure.
- Do not move the blocked-file check into a load flow. A flow that fails leaves Auto Loader's checkpoint behind and the file is read again next run. The check (`<pipeline_id>_blocked_check`) must stay a separate dataset that runs after bronze and quarantine, and the working tables (`_files`, `_blocked_check`, `_arrivals`) live in the pipeline's own schema (`framework_meta`), not beside bronze.
- Keep the staging step shared. `pipeline.stage_frame` is what both the pipeline and `framework_backfill` run on a row, so a backfilled file is judged exactly as a normal load would. Change it in one place.
- The hook writes `load_ledger`, `ledger_detail` at the same time (`write_rows`), because a failed update shuts the pipeline down within seconds and cancels a write still running. Do not make them sequential. A micro-batch of a backfill runs in a worker process: it may use only what it is given and must hand results back through tables, never through variables.
- A volume's files are listed with `volume_listing.list_volume_files`, not Spark's `binaryFile` reader, which leaves out a file with no bytes.
- If you drop and recreate a source volume, run a `full_refresh` backfill of its tables: the private staged table's Auto Loader state still points at the old files and the next update fails with `CF_PARTITON_INFERENCE_ERROR`.
- Do not schedule a `VACUUM` that shortens retention.
- Do not create a table or schema in a spec's name from a pipeline. Schemas and volumes are prepared outside the framework.
- Add a Unity Catalog tag key only by listing it in `SCHEMA_TAGS` or `TABLE_RECORD_TAGS` in `unity.py` and in `v_metadata_drift`. `unity.py` unsets only listed keys, so tags set by others stay.
- Do not write `asset_registry` except through `append_definitions` and `retire_definitions`. They keep exactly one current version per `(object_kind, full_name)`; a plain `INSERT` breaks that.
- Do not drop the `event_log` block, the `lff.manifest` / `lff.catalog` settings, or the `lakeflow_framework_workflow` tag from generated pipelines and jobs. The pipeline, its event hook and `v_loads`, and the run views depend on them.
- Do not reintroduce retired meta objects (`load_metric`, `file_ledger`, `curated_progress` / `silver_progress`, `v_loads_90d`, `v_bronze_tables`, `run_ledger`, `run_current`, `v_process`, `v_run_current`, `v_bronze_loads`, `load_daily_rollup`, ops `flow_events` / `table_load_metrics`). Prefer `ledger_detail` and `flow_checkpoint`. See [META_MODEL_REVIEW.md](META_MODEL_REVIEW.md).

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
