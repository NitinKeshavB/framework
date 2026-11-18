# Lakeflow framework, end to end

The whole path in this repository, from a spec to a registry row. A team adding a table needs only the first section. The rest is for maintainers. The same flows are drawn, with every table's columns and keys, in [ARCHITECTURE.md](ARCHITECTURE.md#flows) and [its data model](ARCHITECTURE.md#data-model).

## What a team does

1. Add a table spec under `framework_ingestion/sources/<source_system>/<project>/`, or directly under `sources/<source_system>/` when no project is bringing it in. `./run.sh new-table` writes it from a template. A new source system needs its `source.yaml` first (`./run.sh new-source`).
2. Replace every `TODO`, then run `./run.sh generate`.
3. Open a pull request. The source system's owner reviews it, since every table in a delivery shares one pipeline.
4. Silver, gold, or a data product: add it to a project's `project.yaml` under `framework_curation` and run `./run.sh projects:generate`.

CI runs `./run.sh lint:ci` (Ruff `E,F,I,B,S,UP` plus hygiene and local generate hooks), mypy on `framework_lff_lib/lakeflow_framework/`, `test:quick`, `generate:check`, and `projects:check`. See [MAINTAINERS.md](MAINTAINERS.md#pre-commit). None of them start a cluster.

## What `generate` produces

For each delivery of each source system, from the specs alone:

- `pipelines/<source_system>_<delivery>.py` calls `build_pipelines([...])` once, with every table in that delivery. The registry is read in one query.
- `.build/resources/<source_system>_<delivery>.pipeline.yml` is one serverless Lakeflow pipeline (regenerated on deploy; not committed).
- `.build/resources/<source_system>_<delivery>.job.yml` is one job. A file source fires on files landing in the raw volume. A scheduled source uses its cron.
- `.build/manifests/<source_system>_<delivery>.json` is the compiled delivery config the pipeline reads via `lff.manifest`.
- `SOURCE_MAP.md` shows the same model as a diagram and tables (committed; `generate:check` drifts this only).

Generated files start with a marker line. `generate` rewrites them, deletes ones no source asks for, and never touches a file without the marker. `bundle:validate` and `bundle:deploy` run it first, so the map, the pipelines, and the jobs cannot disagree with the specs.

## What deploy does

Definitions change only when specs change, and specs change only at deploy, so the registry is written then.

1. `bundle:deploy framework_bundle` runs `framework_ddl`, which creates or migrates `framework_meta` and the run tables and views, then `framework_system_views`, which creates the cost and observed lineage views only when the target sets `system_views` to `true`.
2. `bundle:deploy framework_ingestion` regenerates, uploads the bundle, and runs `registry_sync`. Its one task validates every spec under the target catalog and:
   - writes each source system, schema, workflow, pipeline, dataflow, flow, securable, link, and column into `asset_registry` and `asset_registry_current`, and retires what the bundle no longer defines. An unchanged definition is not a new history row. A table or pipeline another owner holds is refused and recorded in `ownership_refusals`;
   - would set the schema, table, and column tags and column comments in Unity Catalog (see [Unity Catalog tags](#unity-catalog-tags)), but phase 1 parks that step: tag policies on the same keys refuse the deploy identity. Declared tags stay in the registry and in `v_metadata_drift`. Set `APPLY_TAGS` in `unity.py` once the account grants assignment (or drops the policies).

If `registry_sync` fails, the deploy command fails.

Every framework job task writes timestamped log lines to its run's output: the arguments it ran with, each step with its start, end, seconds taken, and the error that ended it, and what it decided. `framework_ddl` logs each statement and its time, or that the SQL was already applied. `registry_sync` logs the definitions it found by kind, each new or changed one, each refusal with the owner that holds the name, each retired definition, each Unity Catalog change, and each schema or table not yet created. `framework_runs` logs the read window, the runs read per job, and any registered workflow whose job it cannot see. `framework_optimize` logs the files each `OPTIMIZE` removed and added, and how many hot rows each archive move transferred.

## What a delivery job does

A delivery job runs **refresh_pipeline**, then **record_load_metrics** with `run_if: ALL_DONE` (runs even when the pipeline failed), then **record_this_run** (NRT job status into `job_runs`). Metrics append one `load_ledger` row per registered dataflow for the job (`layer = bronze`), left-joined to that update's `flow_progress` (AUTO CDC upserts/deletes when `num_output_rows` is absent). Missing progress, a missing event log, or only some tables emitting metrics → **FAILED**. Each row stores `{{job.run_id}}` so `v_load_failures` equi-joins. Both `load_ledger` and `job_runs` / `job_task_runs` are insert-only (no MERGE); `v_loads` / `v_job_runs` keep the latest `recorded_at` per key. Use `v_loads` for recon and `v_load_failures` for what failed and why. To resume after a failure: fix the cause, then **Repair** the failed Databricks job run (failed tasks only); Auto Loader continues from its checkpoint. Hourly `framework_runs` is catch-up. The job does not write definitions to the registry.

The pipeline reads its delivery from the deployed bundle. The generated pipeline sets `lff.manifest` to a compiled JSON under `.build/manifests/` (legacy `lff.specs` still works) and `lff.catalog` to the target catalog; `build_pipelines` validates with `validate_configs`. A pipeline id missing from the delivery is an error. A pipeline without either setting falls back to reading `framework_meta.v_asset_registry_current`.

Whether a run succeeded and how long it took come from the Jobs API and the pipeline event log. Each delivery job writes NRT status via `record_this_run`; hourly `framework_runs` is catch-up into `job_runs`, `job_task_runs`, and `pipeline_updates`. Read them through `v_job_runs`, `v_job_task_runs`, `v_pipeline_updates`, and `v_job_status`. Row counts for recon are in `v_loads`. Cost needs the optional system views.

## How each kind loads

Each table reads its source once, into a private `<pipeline_id>_staged` table that is not published to Unity Catalog. A pipeline cannot send one read to two tables, and a temporary view runs again for every dataset that reads it, so the staging table is what lets bronze and quarantine share one read. It adds `_dq_failed_rules`, the names of the rules the row failed, so each rule is evaluated once per row. Nothing is dropped with `expect_or_drop`.

- **Bronze** holds only passing rows. It reads the staging table `WHERE size(_dq_failed_rules) = 0`, without that column, through the `<pipeline_id>_valid` temporary view. With `dedup: true` or a database `merge`, Auto CDC reads the same view.
- **Quarantine** reads the staging table `WHERE size(_dq_failed_rules) > 0`, with the column renamed to `failed_rules`.

The cost is one extra Delta write of every row, and the staging table's storage. It is cheaper than listing and parsing the files, or querying the database, a second time.

Each rule is also a warn expectation on the staging table, `NOT array_contains(_dq_failed_rules, '<rule>')`, so pass and fail counts reach the pipeline event log without evaluating the rule again.

| Kind and method | Reads | Quarantine | Bronze |
| --- | --- | --- | --- |
| Files, `volume_files` | Auto Loader on the raw volume path, once, into a private staging streaming table. No checkpoint or `schemaLocation` is set; the pipeline owns that state. | Streaming table of failing rows | Streaming table of passing rows, or Auto CDC on the keys when `dedup: true` |
| Database, `snapshot` | The foreign-catalog table, in batch, once, into a private staging materialized view | Materialized view of failing rows | Materialized view of passing rows |
| Database, `merge` | The same | Materialized view of failing rows | Streaming table updated by `create_auto_cdc_from_snapshot_flow` on `primary_keys` |
| Cloud files, `autoloader` | Auto Loader once into a raw streaming table. The projection, including `_source_file` and `_ingested_at`, is applied on that read. | Streaming table, read from the raw table | Streaming table read from the valid view, or Auto CDC from that view when `dedup: true` |
| Cloud files, `copy_into` | `COPY INTO` a raw table (`force = false`) before the pipeline | Streaming table | Streaming table |
| Cloud files, `ctas` | `CREATE OR REPLACE TABLE` raw | Materialized view | Materialized view |

The three cloud-files rows are frozen. The code is kept and tested, but validation rejects these methods until they run end to end on serverless.

Every published table gets a comment, `table_properties` (`quality` and `lakeflow_framework.pipeline_id`), and liquid clustering: `cluster_by_auto`, unless the spec sets `raw_to_bronze.bronze.cluster_by` (up to four columns). An Auto CDC bronze clusters on its first four primary keys by default. Turn on predictive optimization for the catalog once (`ALTER CATALOG <catalog> ENABLE PREDICTIVE OPTIMIZATION`, which needs the catalog owner); it runs `OPTIMIZE` and `VACUUM` on these tables.

Auto Loader on CSV, delimited text, and JSON gets `cloudFiles.schemaHints` from `columns`, so the declared columns land typed and the first run does not sample files to infer them. A value that does not parse lands in `_rescued_data`, so the `schema_conformance` rule sends it to quarantine. Without file events Auto Loader lists the directory; point `raw_path` at the narrowest folder. Once file events are enabled on the external location behind the volume, set `source.file_events: true` to use `cloudFiles.useManagedFileEvents`.

Excel works with Auto Loader and CTAS but not `copy_into`. `dedup: true` needs `primary_keys` and `watermark_column`, and uses `stored_as_scd_type` integer `1`: the latest row for each key wins. The watermark must be in `columns` with its type. Files land it as a string, so the valid view casts it before Auto CDC orders on it, and a null or uncastable watermark goes to quarantine. Keys also generate not-null rules when dedup is off.

## Data quality

Rules in the spec become SQL predicates before a cluster starts.

- A declared rule is the expression you wrote.
- Each primary key gets `column IS NOT NULL`.
- Each declared type gets `TRY_CAST(column AS type) IS NOT NULL OR column IS NULL`, so a null is not also a cast failure.
- Auto Loader on CSV, delimited text, JSON, and text adds `_rescued_data IS NULL`.

A NULL predicate counts as a failure: `COALESCE((predicate), false)`. Quarantine rows carry `failed_rules`, the names of every predicate that failed.

## The registry

Core registry tables in `<catalog>.framework_meta`: SCD2 `asset_registry` (every version, with stable `asset_id`), `asset_registry_current` (one row per key), and insert-only `ownership_refusals` for definitions another owner already holds. `registry_sync` writes them at deploy, and the deploy fails if it fails, so the deployed configuration is always in the tables. The DDL lives in [framework_bundle/sql/framework_meta.sql](../framework_bundle/sql/framework_meta.sql) and ships as package data for wheel installs. Until go-live, a table change means dropping the schema and deploying again. See [MAINTAINERS.md](MAINTAINERS.md) and [META_MODEL_REVIEW.md](META_MODEL_REVIEW.md).

`framework_ddl` records the hash of the SQL it applied in `framework_meta.ddl_applied`. A deploy that brings the same SQL finds the hash and stops after that one lookup. A deploy that changes the SQL file runs every statement again; they are all idempotent. To reapply unchanged SQL, for example after a view was dropped by hand, delete the rows from `ddl_applied` or run `apply_ddl.py` with `force` as its third argument. `framework_system_views` uses the same table for its own file.

`asset_registry` catalogs every definition the job syncs. Each row keeps the definition as `config_json`, the text its `content_hash` covers, and as `config`, the same document parsed to `VARIANT`; the views read `config:path`.

| Kind | What it is |
| --- | --- |
| `source_system` | One `source.yaml`: domain, owner email, source kind, engine, foreign catalog, raw volume, and each delivery with its schedule or trigger and job name. |
| `schema` | A `<catalog>.<schema>` the source reads or writes, with its layers and the tags set on it. |
| `workflow` | The delivery job. `config_json` lists every `pipeline_id`, every requester, the source system and kind, and the delivery's schedule or trigger. |
| `pipeline` | One table load. Holds the canonical spec and compiled quality rules. |
| `dataflow` | Raw path or table, bronze, and quarantine for that load. |
| `flow` | One layer: raw (volume path or table), bronze, or quarantine. |
| `streaming_table`, `table`, `materialized_view` | The securable the flow writes or rejects into. |
| `link` | Edge from the pipeline to a securable. |
| `column` | One column of a securable, with type, description, classification, and PII. |

A new version is written when that object's own hash changes. The job name is a table tag, so a job rename versions the workflow, the pipeline, and its tables, and leaves an unchanged column alone. `parent_full_name` and `relation` (`owns`, `contains`, `reads`, `writes`, `rejects_to`, `column_of`) are the edges. The layout is drawn in [ARCHITECTURE.md](ARCHITECTURE.md#registry-logical).

Every row has an **owner**: `owner_kind` is `source_system` or `project`, and `owner` is its name. Bronze rows are owned by the source system. `requested_by` is the team that asked for the table and `delivery` is how its source delivers. They are for the maps and do not affect ownership.

`ownership_refusals` holds each refused definition: the refused owner, the holder, `failure_reason`, and `content_hash`. Delivery jobs do not write registry steps.

### Ownership

A pipeline or table has one owner. Before inserting, `registry_sync` looks up the current owner of each pipeline and table it deploys. If another owner holds it, the row is not inserted and a row goes to `ownership_refusals`, with the holder named in `failure_reason`. The other definitions are still written. Flows, links, workflows, dataflows, and columns follow their pipeline or table. `v_ownership_conflicts` lists a name that has two rows in `asset_registry_current`; it should be empty. Moving a table to another owner is a deliberate manual change, not something `registry_sync` allows.

### Views

| View | What it answers |
| --- | --- |
| `v_asset_registry_current` | The current definition for every object, read from `asset_registry_current`. This is what the pipeline loads. |
| `v_asset_registry_history` | Every version, with `valid_from`, `valid_to`, `is_current`, and `is_deleted`. A point in time is `valid_from <= t AND (valid_to IS NULL OR valid_to > t) AND NOT is_deleted`. |
| `v_source_systems` | Each current source system as its `source.yaml` declares it, with its deliveries. |
| `v_schemas` | Each schema a source uses, its layers, and the tags set on it. |
| `v_workflows` | Each delivery job, its pipeline ids, every requester, its source, and its schedule or trigger. |
| `v_pipelines` | Owner, requester, delivery, domain, team, technical owner, mail lists. |
| `v_dataflows` | Raw path or table, bronze, and quarantine for each table load. |
| `v_flows` | Each layer flow with relation, path, or table. |
| `v_tables` | Each securable's domain, team, technical owner, classification, PII, layer, pipeline, job, and load method. |
| `v_columns`, `v_column_history` | Current and past columns of raw, bronze, and quarantine. |
| `v_lineage` | Latest link per parent, child, and relation. |
| `v_ownership_refusals` | Every definition `registry_sync` refused because another owner holds the name, with the holder in `failure_reason`. |
| `v_job_status` | Each workflow and its latest run from `v_job_runs`: `RUNNING`, `SUCCEEDED`, `FAILED`, or `SKIPPED`, with the termination code. NRT via `record_this_run`; hourly `framework_runs` is catch-up only. |
| `v_ownership_conflicts` | Names with more than one owner. Empty when the rules hold. |
| `v_loads`, `v_load_failures` | Per-load recon (NRT via `record_load_metrics`) and failure reasons for repair. |
| `v_bronze_tables` | Every bronze table and its owner. |
| `v_metadata_drift` | Each declared schema, table, or column tag, or column comment, that Unity Catalog (`<catalog>.information_schema`) lacks or holds differently. `securable` says which. Empty once `registry_sync` has run after the schemas and tables exist. |

Example: the latest status for every billing job.

```sql
SELECT job_name, status, termination_code, run_id, started_at, run_seconds
FROM framework_meta.v_job_status
WHERE owner = 'billing'
ORDER BY job_name
```

The Jobs API and the event logs are the source for job runs and pipeline updates, and the system tables, where granted, for cost and observed lineage. The registry adds the owner, the requester, the flow graph, and the declared columns. Analysts query `framework_meta` only.

## Metrics

Pipeline event logs give row and rule counts and pipeline update states, the Jobs API gives job runs, the system tables, when granted, give cost and observed lineage, and `DESCRIBE` gives table size. Everything lands in, or is viewed from, `framework_meta`:

| Table or view | Built by | What it holds |
| --- | --- | --- |
| `<job_name>_event_log` | Each ingestion pipeline (`event_log` on the generated resource) | The pipeline's own event log, published to Unity Catalog |
| `load_ledger`, `v_loads`, `v_loads_90d` | Each job, `record_load_metrics` (`ALL_DONE`) | Central recon SoR: one row per registered dataflow (FAILED when event log has no progress for it). `v_loads_90d` unions 7-day hot with forever archive for dashboards |
| `v_load_failures` | Views over loads + job/task runs | What failed and why (`failure_reason`, task, run page) for repair |
| `job_runs`, `v_job_runs`, `v_job_status` | Delivery job `record_this_run` (NRT) + hourly catch-up | Append-only run events; `v_job_runs` keeps latest per `(job_id, run_id)` |
| `job_task_runs`, `v_job_task_runs` | NRT + hourly catch-up | Append-only task events; view keeps latest per `(job_run_id, task_run_id)` |
| `pipeline_updates`, `v_pipeline_updates` | Hourly `framework_runs` from `update_progress` in each `<job>_event_log` | Each pipeline update: start, end, last state, and status |
| `v_job_costs`, `v_pipeline_costs` | `framework_system_views`, only when `system_views` is `true` | DBUs and list cost per job run and per pipeline update, from `system.billing.usage` and `list_prices` |
| `v_observed_lineage` | `framework_system_views`, only when `system_views` is `true` | What `system.access.table_lineage` saw each bronze and quarantine table read, next to the declared raw source: `declared`, `undeclared`, or `not_observed`. The registry's link and flow rows stay the declared intent. |

Quality rules are checked on the private staging table; expectation counts on the ledger come from that update's `flow_progress` when present. Recon SoR is `load_ledger` / `v_loads` (NRT on each delivery job).

Deployed job and pipeline names carry a prefix (`[dev <user>] ` in dev, `[fdh] ` in prod). `framework_runs` matches a job on its `lakeflow_framework_workflow` tag, which the generator sets to the job name, and a pipeline update on the name of the event log it is in. Prefer granting that identity `SELECT` on `system.lakeflow` so catch-up is one MERGE; without it the Jobs API fallback needs `CAN_VIEW` on the delivery jobs. The Jobs API keeps 60 days of runs, and `job_runs` keeps them after that.

**Trust model for quality rules.** Rule names are identifiers. Rule expressions are boolean SQL over that table's columns, literals, and an allow-listed function set — subqueries, comments, and DDL/DML are rejected. They still execute as the pipeline's `run_as` identity. Set `run_as` on each `source.yaml` to that source's service principal and grant it only its own raw volume, foreign catalog, and bronze schema so a careless expression cannot reach another team's data. Without `run_as`, the job runs as the deploying human.

**Per-spec isolation.** `build_pipelines` validates and registers each table on its own. One bad or missing spec is logged and skipped; healthy tables in the same delivery still load. `record_load_metrics` then appends `FAILED` for registered dataflows with no event-log progress.

```sql
-- rows read and loaded for any layer (append-only ledger, not a table scan)
SELECT job_name, layer, target_table, quarantine_table, started_at,
       rows_read, rows_written, rows_quarantine, rows_processed, expectations
FROM framework_meta.v_loads
WHERE target_table ILIKE '%finance_abs'
ORDER BY started_at DESC;
```

A delivery run does not touch the registry, so the number of jobs running at once is limited only by the workspace's pipeline quota.

### Cost and observed lineage, optional

These need grants a metastore admin gives: `USE CATALOG` on `system`, and `USE SCHEMA` and `SELECT` on `system.billing` for cost and `system.access` for lineage. Hourly job-run catch-up separately prefers `SELECT` on `system.lakeflow` (falls back to the Jobs API without it). Once billing/access are granted to the deploying identity, set the bundle variable on that target and deploy `framework_bundle` again:

```yaml
targets:
  dev:
    variables:
      system_views: "true"
```

`framework_system_views` then creates `v_job_costs`, `v_pipeline_costs`, and `v_observed_lineage`. While `system_views` is `false`, its condition task skips the DDL and the run succeeds. Cost is usage at list price. Anyone who can read `framework_meta` can read these views, cost included.

## Physical layout

The tables use liquid clustering:

- `asset_registry`: `CLUSTER BY (owner, object_kind, full_name)`
- `asset_registry_current`: `CLUSTER BY (owner, object_kind, full_name)`
- `ownership_refusals`: `CLUSTER BY (full_name, recorded_at)`

Each table clusters on at most three keys.

All set `delta.autoOptimize.optimizeWrite`, and the registry tables also set `autoCompact`: they are written once per deploy, so compacting after that commit is cheap. Predictive optimization, once the catalog owner enables it, also compacts these tables in the background. A daily job runs `OPTIMIZE` as well, which skips files that are already a good size. `VACUUM` is not scheduled, so process history is never removed. Bloom-filter indexes are not created; liquid clustering is the prune path, and a bloom filter on every small commit is a second write.

`v_asset_registry_history` reads `valid_from` and `valid_to` from `asset_registry`, so a filter on `owner` or on the dates prunes files directly. `asset_registry_current` keeps one row per `object_kind` and `full_name`, so a pipeline id resolves to one document even when the caller does not pass the owner.

`registry_sync` writes a version when `asset_registry_current` has no row or a different hash. One `MERGE` closes the current version (`valid_to`, `is_current = false`) and inserts the new one; then the object's row in `asset_registry_current` is replaced. A retried deploy finds the same hash current and writes nothing to the history. A definition the bundle no longer deploys is retired: its version is closed, a deletion version (`is_deleted = true`) becomes current, and it leaves `asset_registry_current`. Retirement is scoped to the owners this bundle deploys (so ingestion can shard per source); a deploy with no definitions retires nothing. Deploys of one bundle run one at a time, so the last deploy wins. A run joins the definition that was active on `owner`, `object_kind`, and `full_name`, with `started_at` inside `[valid_from, valid_to)`.

To see the registry as it was at a moment:

```sql
SELECT object_kind, full_name, config, valid_from, valid_to
FROM framework_meta.v_asset_registry_history
WHERE owner = 'billing'
  AND valid_from <= TIMESTAMP '2026-11-08 00:00:00'
  AND (valid_to IS NULL OR valid_to > TIMESTAMP '2026-11-08 00:00:00')
  AND NOT is_deleted
```

Deploying the same document again does not insert. Putting an older document back in force does insert, so the days in between still resolve to the version that was active then.

## Unity Catalog tags

`registry_sync` is built to set the tags at every deploy, after it writes the registry. Phase 1 leaves `APPLY_TAGS` false, so that step is a no-op until tag-policy grants are in place. When enabled, it reads `information_schema` first and changes only what differs.

- Each schema the source uses: `domain`, `owner`, `owner_email`, `source_kind`, `layer`, `foreign_catalog`, and `engine`, from `source.yaml`.
- Each table: `classification`, `domain`, `pii`, `data_domain`, `team`, `technical_owner`, `layer`, `pipeline_id`, `job_name`, `load_method`, `owner`, `requested_by`, and `delivery`.
- Each column: `classification` and `pii`, and `description` as its comment.
- Each generated job carries `lakeflow_framework_workflow` and the source's `domain`, `owner`, `owner_email`, `source_kind`, `delivery`, `requested_by`, and `data_domain`. The bundle deploy sets these; they are in the generated `<job>.job.yml`.

A schema or table that does not exist yet, because its pipeline has not run, is tagged at the next deploy. A tag the spec no longer gives is unset; other tags are left alone. Owner email addresses are tags, and Databricks stores tags as plain text that may be replicated across regions.

## Checked on a real workspace

None of the following has run on Databricks. The code assumes them:

- `from pyspark import pipelines` in a Lakeflow pipeline, including `create_auto_cdc_flow`, `create_auto_cdc_from_snapshot_flow`, and `expect`.
- `stored_as_scd_type=1` as an integer.
- A streaming view read by name from a downstream streaming table.
- Auto Loader with `_metadata.file_path` on a volume path.
- A file-arrival trigger on a Unity Catalog volume path, and `${workspace.file_path}` spec paths on serverless.
- Serverless `environment_version` 4 with the wheel as a dependency, and `performance_target: STANDARD` on generated jobs (override per source with `PERFORMANCE_OPTIMIZED` when cold starts matter).
- Federated tables as a pipeline source, and `merge` as a snapshot flow.
- COPY INTO as a job statement before the pipeline.
- Excel `headerRows` and `dataAddress` without `schemaLocation`.
- `DESCRIBE DETAIL` `numRecords` on streaming tables and materialized views.
- Reading `${workspace.file_path}` manifests from a serverless pipeline, and `spark.conf.get` of `lff.manifest` inside it.
- Workspace concurrency if many pipelines run together.
- `event_log` on a serverless pipeline resource, a streaming read of an event log table, and the `flow_progress` field names `record_load_metrics` / `v_loads` read.
- `databricks-sdk` on serverless environment version 4 without listing it as a dependency, `WorkspaceClient()` with the job's own credentials, and `jobs.list` returning `settings.tags` without `expand_tasks`.
- The Jobs API run fields `framework_runs` reads: `state.life_cycle_state` and `result_state`, `status.termination_details.code`, `setup_duration`, `queue_duration`, `execution_duration`, and `run_duration`.
- `details:update_progress.state` on the published event log, and `dlt_update_id` in `usage_metadata` for the optional cost view.
- A condition task with a bundle variable as its left operand, and a job whose only other task is excluded finishing as succeeded.
- `ALTER STREAMING TABLE` and `ALTER MATERIALIZED VIEW` with `SET TAGS`, `UNSET TAGS`, and `ALTER COLUMN ... COMMENT` on pipeline-owned tables, and whether a later pipeline update keeps column comments it does not define.
- `ALTER SCHEMA ... SET TAGS` and `UNSET TAGS`, and `information_schema.schemata` and `schema_tags`, run as the deploy identity.
- The SCD Type 2 `MERGE` with a `UNION ALL` source and a null merge key, `LEFT ANTI JOIN` against a temporary view inside `MERGE`, and `DELETE ... WHERE NOT EXISTS` on `asset_registry_current`. The statements parse and behave as intended on DuckDB, not yet on Databricks.

If one fails, record the observed syntax here and change the single strategy or DDL statement. Do not spread a workaround across the bundles.

## Read next

- [README.md](README.md) for every YAML contract
- [MAINTAINERS.md](MAINTAINERS.md) for versioning and deploy
- [ARCHITECTURE.md](ARCHITECTURE.md) for the package boundaries
- [ROADMAP.md](ROADMAP.md) for what is next
