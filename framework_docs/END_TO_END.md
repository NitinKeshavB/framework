# Lakeflow framework, end to end

This is the map of the framework in this repository. A consuming team does not need the sections on the collector or the package internals to land a file. Those sections are here so a maintainer can see the whole path.

## What a consuming team does

1. Add the wheel artifact to the bundle. `databricks bundle deploy` builds `framework_src` and uploads `dist/*.whl`. Jobs and pipelines reference that file. The events volume is for run status, not for the wheel.
2. Add one bronze YAML file per pipeline under `configs/bronze/`. See [framework_examples/consumer/configs/bronze](../framework_examples/consumer/configs/bronze). `layer: bronze` names the load. `source_to_raw` lands files in raw. `raw_to_bronze` publishes raw to bronze. `quarantine` receives the rows that fail the bronze rules.
3. Add a one-line pipeline file:

```python
from lakeflow_framework import build_pipeline

build_pipeline("billing_invoices")
```

4. Run `lakeflow-framework validate configs/` in CI. This does not start a cluster. An unresolved `${...}`, a secret-looking key, Excel with `copy_into`, dedup without keys and a watermark, or two pipelines writing the same table fails the command and prints every error.

The example bundle [framework_examples/consumer](../framework_examples/consumer) has a `databricks.yml` with dev and prod targets, a `catalog` variable, and resource files under `resources/`. Dev uses development mode. Prod uses production mode and a shared workspace path. Sample tests for those bronze configs are in `framework_examples/consumer/tests`.

## What happens on a run

The consumer job writes a small JSON file at each step. It does not write the registry tables.

1. **mark_running** writes `RUNNING` before anything else, so a refresh during the load already shows the run.
2. **sync** validates YAML and writes one definition file per pipeline, flow, securable, and link. The file name includes the document hash, so an unchanged config overwrites the same file and does not become a second history row.
3. **await_registry** reads `v_object_current` until those pipeline rows are visible. It does not write the table. The pipeline tasks start after that.
4. **batch_load** runs `COPY INTO` or `CREATE OR REPLACE TABLE` when the load method needs it. Auto Loader does nothing in this task.
5. **pipeline** calls `build_pipeline`. The pipeline reads its config from `framework_meta.v_object_current`. A missing row is an error.
6. **record_success** writes `SUCCEEDED`. Pass `--recon-json` with the source and target counts when the task has them. **record_failure** writes `FAILED` with the task error if any earlier task fails. The running step is left in place.

Step files land in `/Volumes/<catalog>/framework_meta/framework_events` as `{project}/{full_name}/{run_id}_{step_no}_{status}.json`. Definition files land under `{project}/definitions/`. One collector job is the only writer of `object_registry` and `run_ledger`. It inserts a definition only when `(object_kind, full_name, content_hash)` is new, and a step only when `(run_id, step_no, status)` is new.

## Where the rows go

Every config in this version is a bronze load. `source_to_raw.method` chooses how files reach the raw table. `raw_to_bronze` reads that table and writes bronze. Rejected rows go to the quarantine table with the failing rule names. Nothing is dropped with `expect_or_drop`.

| Method | Use it when | Raw | Bronze |
| --- | --- | --- | --- |
| `autoloader` (default) | Files keep arriving | Streaming table, `cloudFiles`. No checkpoint path and no `schemaLocation`. The pipeline owns that state. | Streaming table, or Auto CDC when `dedup: true` |
| `copy_into` | A scheduled drop must be loaded once | `COPY INTO` with `force = false`, then the pipeline streams that table | Streaming table |
| `ctas` | A small extract replaces the previous one | `CREATE OR REPLACE TABLE` | Materialized view, because a full replace is not an append |

Excel is allowed for Auto Loader and CTAS. `copy_into` rejects it because COPY INTO has no Excel reader. Fixed-width is a text file plus `fixed_columns` (`name`, `start`, `length`) projected with `substring`. YAML is one `content` string in this version. CSV, delimited text, JSON, Parquet, Avro, and Excel use the option map kept from the old Auto Loader handler.

`dedup: true` requires `primary_keys` and `watermark_column`. Bronze then uses `create_auto_cdc_flow` with `stored_as_scd_type` integer `1`, so the latest row for each key wins. Raw and quarantine are not deduplicated. Keys still generate not-null rules when dedup is off.

Paths must be `abfss://` or `s3://`. Name the Unity Catalog external location in `external_location`. Do not put keys or passwords in the YAML.

## Data quality

Rules in YAML become SQL predicates before a cluster starts.

- A declared rule is the expression you wrote.
- Each primary key gets `column IS NOT NULL`.
- Each declared type gets `TRY_CAST(column AS type) IS NOT NULL OR column IS NULL`, so a null is not also a cast failure.
- Auto Loader on CSV, delimited text, JSON, and text adds `_rescued_data IS NULL`.

A NULL predicate counts as a failure: `COALESCE((predicate), false)`. Quarantine rows carry `failed_rules`, the names of every predicate that failed. Rows that pass go to bronze.

## The registry

Two tables in `<catalog>.framework_meta`. History is insert-only. The framework does not `UPDATE` or `DELETE` these rows.

`object_registry` is the catalog of workflows (Lakeflow jobs), tasks, pipelines, flows, securables, and links. A new row is written when the definition hash changes. `config_json` holds the YAML, the compiled rules, and the attributes that used to be separate attribute tables. `parent_full_name` and `relation` (`owns`, `writes`, `reads`, `rejects_to`) replace a relationship table.

`run_ledger` is one row per process step: step number, step name, status (`RUNNING`, `SUCCEEDED`, `FAILED`), start, end, and failure text. The success step can carry `recon_json`: source file names and counts, inserted, updated, deleted, output rows, rejected, and `matched` or `mismatch`. Insert, update, and delete come from Delta commit metrics when the write is a merge. An append records output rows and leaves those three empty rather than inventing them. Rejected is the quarantine record count from `DESCRIBE DETAIL`, not a scan of the table.

Views:

- `v_object_current` — latest definition. This is what the pipeline loads. Filter `object_kind = 'pipeline'` and `project` for how many pipelines a project has.
- `v_run_current` — latest step in the last 7 days. This is the live screen.
- `v_process` — every step of a run. Filter by `run_id` and `event_date`.
- `v_recon` — success steps with the counts exploded.
- `v_lineage` — latest link per parent, child, and relation. Intended links are written at sync. Observed links can be added later from `system.access.table_lineage`.
- `v_pipelines` — one row per pipeline: domain, project, data domain, team, technical owner, and the failure and success mail lists.
- `v_tables` — one row per raw, bronze, and quarantine table: the same ownership fields plus classification and PII. These labels are the ones a later ontology can read. The pipeline does not create tags in Unity Catalog.
- `v_object_history` — every version of every object: pipeline, workflow, flow, table, link, and column. `valid_from` is `recorded_at`. `valid_to` is the next version's `recorded_at` for that same object, and null while that version is the latest stored. A point in time is `valid_from <= that time AND (valid_to IS NULL OR valid_to > that time)`.
- `v_columns` — the current column on raw, bronze, and quarantine: data type, description, classification, and PII.
- `v_column_history` — the same intervals as `v_object_history`, limited to columns and with the column fields extracted.

Each object is hashed on its own document. A job rename versions the pipeline and the workflow. It does not copy a new row for a table or column whose own fields did not change. Collecting the same document again does not insert. Putting an older document back in force does insert, so the days in between still resolve to the version that was active then.

System tables are the source for job definitions, run duration, pipeline events, information-schema securables, and observed lineage. The collector copies the fields that would be lost when those tables age out or when an object is dropped: project, the flow graph, the failure text, and the source-file reconciliation. Analysts query `framework_meta` only.

People looking at past runs are not counting bronze or quarantine, and they are not scanning `system.lakeflow`. About 10,000 jobs can write status files at once because those are files, not Delta commits. The collector is the only Delta writer. A burst shows up after that collector drains the files. Running 10,000 pipelines at the same moment is still a workspace quota, separate from this registry.

## Physical layout

Delta has no SQL Server indexes. The tables use liquid clustering:

- `object_registry`: `CLUSTER BY (project, object_kind, full_name, recorded_at)`
- `run_ledger`: `CLUSTER BY (project, event_date, full_name, run_id)`

Both set `delta.autoOptimize.optimizeWrite` and `autoCompact`, so the collector's small inserts are compacted as they land. The DDL also creates bloom-filter indexes on `full_name` and `run_id`. If this runtime rejects that statement, the DDL task skips it and clustering remains. A daily job runs `OPTIMIZE` on both tables. `VACUUM` is not scheduled. Vacuum would only drop files left by compaction. It would not remove process steps.

Before an insert, the collector reads only the current hash for the projects, kinds, and names in that batch. It does not scan history for other projects. A read or a join should filter `project` and `object_kind` (and `full_name` when the object is known) before the `valid_from` / `valid_to` test. A run joined to the definition that was active uses `project`, `object_kind`, and `full_name` as the equality keys, then `started_at` inside `[valid_from, valid_to)`. `event_date` bounds `run_ledger`. A query without those filters is not the supported path.

## What is checked on a real workspace

These are assumptions in the code, not results from a run here:

- `from pyspark import pipelines as dp` on the target runtime.
- `stored_as_scd_type=1` as an integer.
- COPY INTO as a job statement before the pipeline, not inside a pipeline table function.
- A CTAS overwrite followed by a bronze materialized view.
- Excel `headerRows` and `dataAddress` without `schemaLocation`.
- `DESCRIBE DETAIL` `numRecords` on streaming tables and materialized views.
- Bloom-filter index syntax.
- The collector checkpoint under the events volume.
- Whether an `abfss` or `s3` path shows up as `PATH` in `system.access.table_lineage`.
- Workspace concurrency if many pipelines run together.

## Where to read next

- [README.md](README.md) for the consumer contract.
- [MAINTAINERS.md](MAINTAINERS.md) for versioning and publishing the wheel.
- [ARCHITECTURE.md](ARCHITECTURE.md) for the old repository, the dependency direction, and the migration map.
