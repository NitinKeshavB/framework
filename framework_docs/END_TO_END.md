# Lakeflow framework, end to end

The whole path in this repository, from a spec to a registry row. A team adding a table needs only the first section. The rest is for maintainers.

## What a team does

1. Add a table spec under `framework_ingestion/sources/<source_system>/<project>/`, or directly under `sources/<source_system>/` when no project is bringing it in. `./run.sh new-table` writes it from a template. A new source system needs its `source.yaml` first (`./run.sh new-source`).
2. Replace every `TODO`, then run `./run.sh generate`.
3. Open a pull request. The source system's owner reviews it, since every table in a delivery shares one pipeline.
4. Silver, gold, or a data product: add it to a project's `project.yaml` under `framework_curation` and run `./run.sh projects:generate`.

CI runs `./run.sh lint:ci`, `test:quick`, `generate:check`, and `projects:check`. None of them start a cluster.

## What `generate` produces

For each delivery of each source system, from the specs alone:

- `pipelines/<source_system>_<delivery>.py` calls `build_pipelines([...])` once, with every table in that delivery. The registry is read in one query.
- `resources/<source_system>_<delivery>.pipeline.yml` is one serverless Lakeflow pipeline.
- `resources/<source_system>_<delivery>.job.yml` is one job. A file source fires on files landing in the raw volume. A scheduled source uses its cron.
- `SOURCE_MAP.md` shows the same model as a diagram and tables.

Generated files start with a marker line. `generate` rewrites them, deletes ones no source asks for, and never touches a file without the marker. `bundle:validate` and `bundle:deploy` run it first, so the map, the pipelines, and the jobs cannot disagree with the specs.

## What a delivery job does

The job writes small JSON files at each step. It does not write the registry tables.

1. **mark_running** writes `RUNNING` first, so a refresh during the load already shows the run. The step is filed as `object_kind = workflow` and `full_name = <job_name>`.
2. **sync** validates every spec and writes one definition file per workflow, pipeline, dataflow, flow, securable, link, and column. The file name carries the document hash, so an unchanged spec overwrites the same file and is not a new history row.
3. **await_registry** waits until `v_object_current` has the content hash that sync just recorded for each pipeline. A row with an older hash means the spec is out of date, so the job keeps waiting.
4. **batch_load** runs only for `cloud_files` tables that use `copy_into` or `ctas`. Those methods are not in the MVP, so no generated job has this task yet.
5. **refresh_pipeline** runs the Lakeflow pipeline. `build_pipeline` reads each spec back from the registry. A missing row is an error.
6. **record_success** writes `SUCCEEDED` and can carry counts (`--recon-json`). **record_failure** writes `FAILED` if any earlier task fails, including a failed success step.

Files land in `/Volumes/<catalog>/framework_meta/framework_events` as `<owner_kind>/<owner>/<full_name>/<run_id>_<step_no>_<status>.json`, and definitions under `<owner_kind>/<owner>/definitions/`. The collector lands those files. It is paused by default; start it after the first framework deploy so run steps reach the ledger.

Deploy also updates the registry before it returns. `bundle:deploy framework_bundle` runs `framework_ddl`. `bundle:deploy framework_ingestion` runs `registry_sync`, which inserts the deployed specs with the same writer the collector uses. A later delivery sync of an unchanged spec does not add another history row.

## How each kind loads

The graph depends on the kind. Quarantine and bronze are filtered by the quality rules; nothing is dropped with `expect_or_drop`.

| Kind and method | Reads | Quarantine | Bronze |
| --- | --- | --- | --- |
| Files, `volume_files` | Auto Loader on the raw volume path. No checkpoint or `schemaLocation` is set; the pipeline owns that state. | Streaming table of failing rows | Streaming table of passing rows, or Auto CDC on the keys when `dedup: true` |
| Database, `snapshot` | The foreign-catalog table, in batch | Materialized view of failing rows | Materialized view of passing rows |
| Database, `merge` | The same, as snapshots | Materialized view | Streaming table updated by `create_auto_cdc_from_snapshot_flow` on `primary_keys` |
| Cloud files, `autoloader` | Auto Loader once into a raw streaming table. The projection, including `_source_file` and `_ingested_at`, is applied on that read. | Streaming table, read from the raw table | Streaming table read from the valid view, or Auto CDC from that view when `dedup: true` |
| Cloud files, `copy_into` | `COPY INTO` a raw table (`force = false`) before the pipeline | Streaming table | Streaming table |
| Cloud files, `ctas` | `CREATE OR REPLACE TABLE` raw | Materialized view | Materialized view |

The three cloud-files rows are frozen. The code is kept and tested, but validation rejects these methods until they run end to end on serverless.

Excel works with Auto Loader and CTAS but not `copy_into`. `dedup: true` needs `primary_keys` and `watermark_column`, and uses `stored_as_scd_type` integer `1`: the latest row for each key wins. The watermark must be in `columns` with its type. Files land it as a string, so the valid view casts it before Auto CDC orders on it, and a null or uncastable watermark goes to quarantine. Keys also generate not-null rules when dedup is off.

## Data quality

Rules in the spec become SQL predicates before a cluster starts.

- A declared rule is the expression you wrote.
- Each primary key gets `column IS NOT NULL`.
- Each declared type gets `TRY_CAST(column AS type) IS NOT NULL OR column IS NULL`, so a null is not also a cast failure.
- Auto Loader on CSV, delimited text, JSON, and text adds `_rescued_data IS NULL`.

A NULL predicate counts as a failure: `COALESCE((predicate), false)`. Quarantine rows carry `failed_rules`, the names of every predicate that failed.

## The registry

Four tables in `<catalog>.framework_meta`. `object_registry` and `run_ledger` are the history and stay insert-only. `object_current` and `run_current` hold one row per key so a job status check does not window that history. The collector merges those two. The DDL lives in [framework_bundle/sql/framework_meta.sql](../framework_bundle/sql/framework_meta.sql). A catalog created before these tables existed picks them up the next time `framework_ddl` runs; the script copies the latest history row in only while the current table is empty.

`object_registry` catalogs every definition the job syncs:

| Kind | What it is |
| --- | --- |
| `workflow` | The delivery job. `config_json` lists every `pipeline_id` and every requester. |
| `pipeline` | One table load. Holds the canonical spec and compiled quality rules. |
| `dataflow` | Raw path or table, bronze, and quarantine for that load. |
| `flow` | One layer: raw (volume path or table), bronze, or quarantine. |
| `streaming_table`, `table`, `materialized_view` | The securable the flow writes or rejects into. |
| `link` | Edge from the pipeline to a securable. |
| `column` | One column of a securable, with type, description, classification, and PII. |

A new row is written when that object's own hash changes, so a job rename versions the pipeline and workflow and leaves an unchanged table or column alone. `parent_full_name` and `relation` (`owns`, `contains`, `reads`, `writes`, `rejects_to`, `column_of`) are the edges. The layout is drawn in [ARCHITECTURE.md](ARCHITECTURE.md#registry-logical).

Every row has an **owner**: `owner_kind` is `source_system` or `project`, and `owner` is its name. Bronze rows are owned by the source system. `requested_by` is the team that asked for the table and `delivery` is how its source delivers. They are for the maps and do not affect ownership.

`run_ledger` is one row per step: number, name, status (`RUNNING`, `SUCCEEDED`, `FAILED`), start, end, and failure text. Delivery jobs file steps with `object_kind = workflow` and `full_name = <job_name>`. The success step can carry `recon_json`: counts, inserted, updated, deleted, output rows, rejected, and `matched` or `mismatch`. An ownership refusal is a `FAILED` `ownership_check` step on the pipeline or table that was refused.

### Ownership

A pipeline or table has one owner. Before inserting, the collector looks up the current owner of each pipeline and table in the batch. If another owner holds it, the row is not inserted and the collector writes a `FAILED` `ownership_check` step to the ledger, with the holder named in `failure_reason`. The collector keeps running. Flows, links, workflows, dataflows, and columns follow their pipeline or table. `v_ownership_conflicts` lists a name that has two rows in `object_current`; it should be empty. Moving a table to another owner is a deliberate manual change, not something the collector allows.

### Views

| View | What it answers |
| --- | --- |
| `v_object_current` | The current definition for every object, read from `object_current`. This is what the pipeline loads. |
| `v_object_history` | Every version, with `valid_from` / `valid_to`. A point in time is `valid_from <= t AND (valid_to IS NULL OR valid_to > t)`. |
| `v_workflows` | Each delivery job, its pipeline ids, and every requester. |
| `v_pipelines` | Owner, requester, delivery, domain, team, technical owner, mail lists. |
| `v_dataflows` | Raw path or table, bronze, and quarantine for each table load. |
| `v_flows` | Each layer flow with relation, path, or table. |
| `v_tables` | Classification and PII on each securable. |
| `v_columns`, `v_column_history` | Current and past columns of raw, bronze, and quarantine. |
| `v_lineage` | Latest link per parent, child, and relation. |
| `v_process` | Every step of every run. |
| `v_run_current` | Latest step per job in the last 7 days, read from `run_current`. |
| `v_job_status` | Each workflow joined to that latest step: `RUNNING`, `SUCCEEDED`, or `FAILED`. |
| `v_recon` | Counts on success steps from the last 7 days. Older counts stay in `run_ledger`. |
| `v_ownership_conflicts` | Names with more than one owner. Empty when the rules hold. |

Example: the latest status for every billing job.

```sql
SELECT job_name, status, step_name, run_id, failure_reason, event_date
FROM framework_meta.v_job_status
WHERE owner = 'billing'
ORDER BY job_name
```

System tables remain the source for job definitions, run duration, pipeline events, and observed lineage. The collector copies the fields that would be lost when those age out or an object is dropped: the owner, the flow graph, the failure text, and the source reconciliation. Analysts query `framework_meta` only.

About 10,000 jobs can write status files at once because those are files, not Delta commits. The collector is the only Delta writer. Running that many pipelines at once is a workspace quota, separate from this registry.

## Physical layout

The tables use liquid clustering:

- `object_registry`: `CLUSTER BY (owner, object_kind, full_name, recorded_at)`
- `object_current`: `CLUSTER BY (owner, object_kind, full_name)`
- `run_ledger`: `CLUSTER BY (owner, event_date, full_name, run_id)`
- `run_current`: `CLUSTER BY (owner, object_kind, full_name)`

All four set `delta.autoOptimize.optimizeWrite`. `autoCompact` is off: the collector commits every 30 seconds, and compacting after each commit rewrites the same files and falls behind the jobs waiting on `await_registry`. A daily job runs `OPTIMIZE`, which skips files that are already a good size. `VACUUM` is not scheduled, so process history is never removed. Bloom-filter indexes are not created; liquid clustering is the prune path, and a bloom filter on every small commit is a second write.

`v_object_history` partitions by `owner_kind`, `owner`, `object_kind`, and `full_name`. A query that filters `owner` is applied before that window. Filtering only `valid_from` cannot be, because that would change which row comes next. `object_current` keeps one row per `object_kind` and `full_name`, so a pipeline id resolves to one document even when the caller does not pass the owner.

A sync writes one JSON-lines file per owner. An unchanged sync overwrites that same file, so Auto Loader does not read it again. Step files stay one per status. The collector lands a batch in Spark: it does not collect the rows onto the driver. It appends a definition when `object_current` has no row or a different hash, and a step when `(run_id, step_no, status)` is absent from the last two days of `run_ledger`. It then merges the current tables, and only when the incoming `recorded_at` is at least as new as the row already there. A file that arrives late does not roll the current row backward. The checkpoint directory under the events volume is not ingested. A run joins the definition that was active on `owner`, `object_kind`, and `full_name`, with `started_at` inside `[valid_from, valid_to)`. `event_date` bounds `run_ledger`.

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
- The collector checkpoint under the events volume, with `pathGlobFilter` of `*.json` so checkpoint files are not ingested.
- Workspace concurrency if many pipelines run together.

If one fails, record the observed syntax here and change the single strategy or DDL statement. Do not spread a workaround across the bundles.

## Read next

- [README.md](README.md) for every YAML contract
- [MAINTAINERS.md](MAINTAINERS.md) for versioning and deploy
- [ARCHITECTURE.md](ARCHITECTURE.md) for the package boundaries
- [ROADMAP.md](ROADMAP.md) for what is next
