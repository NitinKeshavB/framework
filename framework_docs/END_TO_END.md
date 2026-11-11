# Lakeflow framework, end to end

The whole path in this repository, from a spec to a registry row. A team adding a table needs only the first section. The rest is for maintainers.

## What a team does

1. Add a table spec under `framework_ingestion/sources/<source_system>/<project>/`, or directly under `sources/<source_system>/` when no project is bringing it in. `./run.sh new-table` writes it from a template. A new source system needs its `source.yaml` first (`./run.sh new-source`).
2. Replace every `TODO`, then run `./run.sh generate`.
3. Open a pull request. The source system's owner reviews it, since every table in a delivery shares one pipeline.
4. Silver and gold: add the table to a project's `project.yaml` and run `./run.sh projects:generate`.

CI runs `./run.sh lint`, `test:quick`, `generate:check`, and `projects:check`. None of them start a cluster.

## What `generate` produces

For each delivery of each source system, from the specs alone:

- `pipelines/<source_system>_<delivery>.py` calls `build_pipeline("<id>")` once for each table.
- `resources/<source_system>_<delivery>.pipeline.yml` is one serverless Lakeflow pipeline.
- `resources/<source_system>_<delivery>.job.yml` is one job. A file source fires on files landing in the raw volume. A scheduled source uses its cron.
- `SOURCE_MAP.md` shows the same model as a diagram and tables.

Generated files start with a marker line. `generate` rewrites them, deletes ones no source asks for, and never touches a file without the marker. `bundle:validate` and `bundle:deploy` run it first, so the map, the pipelines, and the jobs cannot disagree with the specs.

## What a delivery job does

The job writes small JSON files at each step. It does not write the registry tables.

1. **mark_running** writes `RUNNING` first, so a refresh during the load already shows the run.
2. **sync** validates every spec and writes one definition file per pipeline, flow, securable, link, and workflow. The file name carries the document hash, so an unchanged spec overwrites the same file and is not a new history row.
3. **await_registry** waits until the collector has loaded those pipelines into `v_object_current`.
4. **batch_load** runs only for `cloud_files` tables that use `copy_into` or `ctas`.
5. **refresh_pipeline** runs the Lakeflow pipeline. `build_pipeline` reads each spec back from the registry. A missing row is an error.
6. **record_success** writes `SUCCEEDED` and can carry counts (`--recon-json`). **record_failure** writes `FAILED` if any earlier task fails.

Files land in `/Volumes/<catalog>/framework_meta/framework_events` as `<owner_kind>/<owner>/<full_name>/<run_id>_<step_no>_<status>.json`, and definitions under `<owner_kind>/<owner>/definitions/`. One collector job is the only writer of `object_registry` and `run_ledger`.

## How each kind loads

The graph depends on the kind. Quarantine and bronze are filtered by the quality rules; nothing is dropped with `expect_or_drop`.

| Kind and method | Reads | Quarantine | Bronze |
| --- | --- | --- | --- |
| Files, `volume_files` | Auto Loader on the raw volume path. No checkpoint or `schemaLocation` is set; the pipeline owns that state. | Streaming table of failing rows | Streaming table of passing rows, or Auto CDC on the keys when `dedup: true` |
| Database, `snapshot` | The foreign-catalog table, in batch | Materialized view of failing rows | Materialized view of passing rows |
| Database, `merge` | The same, as snapshots | Materialized view | Streaming table updated by `create_auto_cdc_from_snapshot_flow` on `primary_keys` |
| Cloud files, `autoloader` | Auto Loader into a raw streaming table | Streaming table | Streaming table, or Auto CDC with `dedup: true` |
| Cloud files, `copy_into` | `COPY INTO` a raw table (`force = false`) before the pipeline | Streaming table | Streaming table |
| Cloud files, `ctas` | `CREATE OR REPLACE TABLE` raw | Materialized view | Materialized view |

Excel works with Auto Loader and CTAS but not `copy_into`. `dedup: true` needs `primary_keys` and `watermark_column`, and uses `stored_as_scd_type` integer `1`: the latest row for each key wins. Keys also generate not-null rules when dedup is off.

## Data quality

Rules in the spec become SQL predicates before a cluster starts.

- A declared rule is the expression you wrote.
- Each primary key gets `column IS NOT NULL`.
- Each declared type gets `TRY_CAST(column AS type) IS NOT NULL OR column IS NULL`, so a null is not also a cast failure.
- Auto Loader on CSV, delimited text, JSON, and text adds `_rescued_data IS NULL`.

A NULL predicate counts as a failure: `COALESCE((predicate), false)`. Quarantine rows carry `failed_rules`, the names of every predicate that failed.

## The registry

Two tables in `<catalog>.framework_meta`. History is insert-only: the framework never runs `UPDATE` or `DELETE` on them.

`object_registry` catalogs workflows (jobs), pipelines, flows, securables, links, and columns. A new row is written when that object's own hash changes, so a job rename versions the pipeline and workflow and leaves an unchanged table or column alone. `config_json` holds the document for that one object. `parent_full_name` and `relation` (`owns`, `writes`, `rejects_to`, `column_of`) are the edges. The registry layout and the logical model are drawn in [ARCHITECTURE.md](ARCHITECTURE.md#registry-physical).

Every row has an **owner**: `owner_kind` is `source_system` or `project`, and `owner` is its name. Bronze rows are owned by the source system. `requested_by` is the team that asked for the table and `delivery` is how its source delivers. They are for the maps and do not affect ownership.

`run_ledger` is one row per step: number, name, status (`RUNNING`, `SUCCEEDED`, `FAILED`), start, end, and failure text. The success step can carry `recon_json`: counts, inserted, updated, deleted, output rows, rejected, and `matched` or `mismatch`.

### Ownership

A pipeline or table has one owner. Before inserting, the collector looks up the current owner of each pipeline and table in the batch. If another owner holds it, the row is not inserted and the collector writes a `FAILED` `ownership_check` step to the ledger, with the holder named in `failure_reason`. The collector keeps running. Flows, links, workflows, and columns follow their pipeline or table. `v_ownership_conflicts` lists any name with two owners in its history; it should be empty. Moving a table to another owner is a deliberate manual change, not something the collector allows.

### Views

- `v_object_current`: latest definition. This is what the pipeline loads.
- `v_object_history`: every version of every object (pipeline, workflow, flow, table, link, column). `valid_from` is `recorded_at`; `valid_to` is the next version's `recorded_at` for that object, null while it is the latest. A point in time is `valid_from <= t AND (valid_to IS NULL OR valid_to > t)`.
- `v_pipelines`, `v_tables`: owner, requester, delivery, domain, data domain, team, technical owner, mail lists, classification, and PII.
- `v_columns`, `v_column_history`: the current columns of raw, bronze, and quarantine, and every version of them (`valid_from`, `valid_to`, `is_current`).
- `v_lineage`: latest link per parent, child, and relation, written at sync.
- `v_run_current`: latest step in the last 7 days. `v_process`: every step of a run. `v_recon`: counts on success steps.
- `v_ownership_conflicts`: names with more than one owner.

System tables remain the source for job definitions, run duration, pipeline events, and observed lineage. The collector copies the fields that would be lost when those age out or an object is dropped: the owner, the flow graph, the failure text, and the source reconciliation. Analysts query `framework_meta` only.

About 10,000 jobs can write status files at once because those are files, not Delta commits. The collector is the only Delta writer. Running that many pipelines at once is a workspace quota, separate from this registry.

## Physical layout

The tables use liquid clustering:

- `object_registry`: `CLUSTER BY (owner, object_kind, full_name, recorded_at)`
- `run_ledger`: `CLUSTER BY (owner, event_date, full_name, run_id)`

Both set `delta.autoOptimize.optimizeWrite` and `autoCompact`, so the collector's small inserts are compacted as they land. The DDL also creates bloom-filter indexes on `full_name` and `run_id`; if the runtime rejects that, the DDL task skips it. A daily job runs `OPTIMIZE`. `VACUUM` is not scheduled, so process history is never removed.

Before an insert, the collector reads only the current hash for the owners, kinds, and names in that batch, and the current owner of the pipelines and tables in it. It does not scan history for other owners. Filter `owner` and `object_kind` (and `full_name` when known) before the `valid_from` / `valid_to` test. A run joins the definition that was active on `owner`, `object_kind`, and `full_name`, with `started_at` inside `[valid_from, valid_to)`. `event_date` bounds `run_ledger`.

To see the registry as it was at a moment:

```sql
SELECT object_kind, full_name, config_json, valid_from, valid_to
FROM framework_meta.v_object_history
WHERE owner = 'billing'
  AND valid_from <= TIMESTAMP '2026-11-08 00:00:00'
  AND (valid_to IS NULL OR valid_to > TIMESTAMP '2026-11-08 00:00:00')
```

Collecting the same document again does not insert. Putting an older document back in force does insert, so the days in between still resolve to the version that was active then.

## Checked on a real workspace

None of the following has run on Databricks. The code assumes them:

- `from pyspark import pipelines` in a Lakeflow pipeline, including `create_auto_cdc_flow`, `create_auto_cdc_from_snapshot_flow`, and `expect`.
- `stored_as_scd_type=1` as an integer.
- A streaming view read by name from a downstream streaming table.
- Auto Loader with `_metadata.file_path` on a volume path.
- A file-arrival trigger on a Unity Catalog volume path, and `${workspace.file_path}` spec paths on serverless.
- Serverless `environment_version` 2 with the wheel as a dependency.
- Federated tables as a pipeline source, and `merge` as a snapshot flow.
- COPY INTO as a job statement before the pipeline.
- Excel `headerRows` and `dataAddress` without `schemaLocation`.
- `DESCRIBE DETAIL` `numRecords` on streaming tables and materialized views.
- Bloom-filter index syntax, and the collector checkpoint under the events volume.
- Workspace concurrency if many pipelines run together.

If one fails, record the observed syntax here and change the single strategy or DDL statement. Do not spread a workaround across the bundles.

## Read next

- [README.md](README.md) for every YAML contract
- [MAINTAINERS.md](MAINTAINERS.md) for versioning and deploy
- [ARCHITECTURE.md](ARCHITECTURE.md) for the package boundaries
- [ROADMAP.md](ROADMAP.md) for what is next
