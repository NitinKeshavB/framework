# Architecture

How the framework is put together: the folders, the code, every table it writes, and how data and metadata move between them. The run path step by step is in [END_TO_END.md](END_TO_END.md); the YAML contracts are in [README.md](README.md).

## Layout

Folders at the repository root start with `framework_`.

- `framework_lff_lib` is the wheel. Public names are `build_pipeline`, `validate_configs`, and `LoadMethod`.
- `framework_bundle` deploys that wheel and the frozen metadata registry: `sql/framework_meta.sql` and `sql/system_views.sql`, the DDL jobs, hourly `framework_runs` (catch-up for job runs / pipeline updates; delivery NRT is separate), and daily `framework_optimize` (compaction, `archive_hot`). See [META_MODEL_REVIEW.md](META_MODEL_REVIEW.md).
- `framework_ingestion` is source to bronze: source systems, table specs, and the pipelines and jobs generated from them.
- `framework_curation/<domain>/<project>` is one bundle per project for silver, gold, and data products.
- `framework_examples/ingestion` is the set of specs to copy for each source kind. `framework_examples/consumer` is the original single-bundle example and is reference only.
- `framework_tests` holds package tests. They copy sample YAML and use fake Spark objects; they do not start a cluster.
- `framework_docs` is this documentation.

Three things are kept apart on purpose: the **meta framework** (registry and metrics), **source ingestion** (domain-owned, shared), and **curation** (silver, gold, and data products, one project owner each). They deploy separately and have different owners.

## Modules

Each module imports only the ones listed. `ingestion` is a thin re-export of `sources` / `generate` / `scaffold`.

| Module | Imports | What it does |
| --- | --- | --- |
| `sqlnames` | nothing | Identifier checks, SQL string quoting, shared `spark_rows` / `merge_sql` |
| `spark_types` | nothing | Optional `pyspark.sql.types` plus `require_writer` so registry writes never silently no-op |
| `config` | `sqlnames` | Validates specs, joins a spec with its `source.yaml`, and holds `PipelineConfig` |
| `load` | `config`, `sqlnames` | Load strategies: Auto Loader options, `schemaHints`, and batch SQL. No `pyspark` |
| `quality` | `config`, `load` | Rules to SQL predicates, `_dq_failed_rules`, and the warn expectations |
| `pipeline` | `config`, `load`, `quality` | `assemble` builds the dataset graph; `build_pipelines` reads `lff.manifest` (legacy `lff.specs`) and registers with `pyspark.pipelines` |
| `metadata` | `config`, `quality`, `sqlnames` | Registry rows, ownership-refusal payloads, DDL text (package data), and SQL lookups |
| `registry` | `metadata`, `spark_types`, `sqlnames` | The only registry writer: definitions, ownership refusals, retirement, and DDL apply |
| `load_metrics` | `sqlnames` | Appends `load_ledger` / `load_metric` from pipeline event logs (`record_load_metrics`) |
| `unity` | `sqlnames` | Applies schema, table, and column tags and column comments to Unity Catalog |
| `runs` | `sqlnames`, `spark_types` | Records job runs from the Jobs API / `system.lakeflow` and pipeline updates from event logs |
| `sources` | `config`, `metadata`, `unity` | Reads `source.yaml` and specs; builds deploy registry rows |
| `generate` | `config`, `sources`, `unity` | Writes `SOURCE_MAP.md` and `.build/` pipelines, jobs, and delivery manifests |
| `scaffold` | `config`, `sources` | `new-source` / `new-table` templates |
| `ingestion` | `sources`, `generate`, `scaffold` | Public facade used by the CLI, projects, and tests |
| `projects` | `config`, `ingestion` | Validates `project.yaml` and writes the project maps |
| `cli` | all of them, most on first use | `lakeflow-framework` commands, called by `run.sh` and the job tasks |

Load strategies return option dictionaries and SQL strings. Public `LoadMethod` names the MVP methods (`VOLUME_FILES`, `DATABASE_TABLE`) and the frozen ones (`AUTOLOADER`, `COPY_INTO`, `CTAS`). `build_pipeline` touches Spark and `pyspark.pipelines` only when the caller passes nothing, which is the generated one-line pipeline file on a cluster. The CLI validates and writes files without a session. Generated jobs default to `performance_target: STANDARD` (override per source in `source.yaml`).

## Flows

### From a spec to deployed resources

```mermaid
flowchart LR
  spec[source.yaml and table specs] --> generate[generate]
  generate --> pipelineFile["pipelines/&lt;job&gt;.py"]
  generate --> pipelineRes["&lt;job&gt;.pipeline.yml"]
  generate --> jobRes["&lt;job&gt;.job.yml"]
  generate --> map[SOURCE_MAP.md]
  deploy[bundle:deploy] --> registrySync[registry_sync job]
  spec --> registrySync
  registrySync --> registry[(asset_registry SCD Type 2 and asset_registry_current)]
  registrySync --> uc[(Unity Catalog schema, table, and column tags and column comments)]
```

One delivery of one source system is one pipeline and one job, named `<source_system>_<delivery>`. `framework_bundle` deploys first and runs `framework_ddl`, then `framework_system_views`, which does nothing unless the target sets `system_views` to `true`. `framework_ingestion` deploys next and runs `registry_sync`, the only writer of definitions. At every deploy it:

1. Writes every source system, schema, workflow, pipeline, dataflow, flow, table, link, and column the bundle defines. A new or changed definition gets a new version in `asset_registry`, and the version it replaces is closed.
2. Retires what the bundle no longer defines. Each such object gets a deletion version and leaves `asset_registry_current`.
3. Sets the tags on each schema, table, and column, and each column comment, in Unity Catalog. It changes only what differs.

`run.sh deploy` fails when `registry_sync` fails, so a deploy whose metadata did not load is a failed deploy. The generated job also carries tags: the `lakeflow_framework_workflow` tag the run views join on, and the source's `domain`, `owner`, `owner_email`, `source_kind`, `delivery`, `requested_by`, and `data_domain`.

### A delivery run

```mermaid
flowchart TD
  trigger[schedule or file arrival] --> batchLoad["batch_load (COPY INTO and CTAS specs only)"]
  batchLoad --> refresh[refresh_pipeline]
  trigger -. no batch specs .-> refresh
  specs["lff.manifest (compiled delivery JSON)"] --> refresh
  refresh --> tables[(staged, bronze, and quarantine tables)]
  refresh --> eventLog[("&lt;job&gt;_event_log")]
  refresh --> recordMetrics["record_load_metrics (run_if ALL_DONE)"]
  eventLog --> recordMetrics
  recordMetrics --> ledger[(load_ledger)]
  recordMetrics --> recordThisRun[record_this_run]
  recordThisRun --> jobRuns[(job_runs NRT)]
  trigger --> jobsApi[(Jobs API run history)]
```

The job runs **refresh_pipeline**, then **record_load_metrics** with `run_if: ALL_DONE` (even when the pipeline failed), which appends one `load_ledger` row per registered dataflow for the job's medallion `layer`, left-joined to that update's `flow_progress` (FAILED when a dataflow has no progress, including partial delivery failures). Then **record_this_run** writes NRT status and `failure_reason` into `job_runs`. Insert only; no MERGE; no scan of target tables. Frozen `cloud_files` methods add **batch_load** before the refresh. The pipeline validates its deployed spec files itself, so a run neither writes nor waits on the registry for specs. Hourly `framework_runs` is catch-up from the Jobs API and event logs.

### Inside one pipeline: source to bronze and quarantine

```mermaid
flowchart LR
  source[raw volume files or foreign table] -->|read once| staged["&lt;pipeline_id&gt;_staged (private)"]
  staged -->|"size(_dq_failed_rules) = 0"| valid["&lt;pipeline_id&gt;_valid (temporary view)"]
  valid --> bronze[bronze table]
  staged -->|"size(_dq_failed_rules) > 0"| quarantine["quarantine table (_qrntn)"]
```

The staging table computes `_dq_failed_rules` once per row and carries one warn expectation per rule, so pass and fail counts reach the event log. Bronze is an append streaming table, an Auto CDC streaming table (`dedup: true`, or database `merge`), or a materialized view (database `snapshot`). The frozen `cloud_files` methods add a raw table before the staging table.

### Metrics and governance

```mermaid
flowchart LR
  ingestionPipelines[ingestion pipelines] --> eventLogs[("&lt;job&gt;_event_log")]
  anyJob[any layer job record_load_metrics ALL_DONE] --> eventLogs
  anyJob --> ledger[(load_ledger)]
  ledger --> loads[v_loads]
  loads --> failures[v_load_failures]
  anyJob --> recordThisRun[record_this_run]
  recordThisRun --> runTables[(job_runs, job_task_runs)]
  jobsApi[(Jobs API)] --> runsJob[framework_runs, hourly catch-up]
  eventLogs --> runsJob
  runsJob --> runTables
  runsJob --> updates[(pipeline_updates)]
  runTables --> runViews[v_job_runs, v_job_task_runs, v_job_status]
  updates --> runViews
  registry[(asset_registry_current)] --> runViews
  systemTables[("system.billing, system.access (optional)")] --> costViews[v_job_costs, v_pipeline_costs, v_observed_lineage]
  runTables --> costViews
  optimize[framework_optimize, daily] --> archive[archive_hot to *_archive]
  ledger --> archive
  runTables --> archive
  archive --> loads90[v_loads_90d]
  registry --> metadataDrift[v_metadata_drift]
  infoSchema[(catalog information_schema)] --> metadataDrift
```

## Data model

Editable Lucidchart (tabs: ERD, **framework_meta catalog**, **Team process: patterns → jobs**): [Lakeflow framework_meta](https://lucid.app/lucidchart/4713f75e-9ce7-407b-a31c-5444a17b9ec9/edit). Purpose and usage of every table and view: [framework_meta_data_dictionary.html](framework_meta_data_dictionary.html) (open in a browser; use **Download HTML** for an offline copy).

### Every table, by owner

| Table | Schema | Written by | Grain | Kind |
| --- | --- | --- | --- | --- |
| `asset_registry` | `framework_meta` | `registry_sync` | One version of one definition (`bundle_id` = which deploy wrote it) | Delta, SCD Type 2 |
| `asset_registry_current` | `framework_meta` | `registry_sync` | One row per `(object_kind, full_name)` | Delta, merged |
| `{job}_event_log` | `framework_events` | each delivery pipeline | Lakeflow event log (out of `framework_meta` so SHOW TABLES scales) | Delta |
| `ownership_refusals` | `framework_meta` | `registry_sync` | One refused definition (another owner already holds the name) | Delta, insert-only |
| `job_runs` | `framework_meta` | each job (`record_this_run`) + hourly `framework_runs` | One append per observed run state (retries OK) | Delta, append-only; `v_job_runs` keeps latest `(job_id, run_id)` |
| `job_task_runs` | `framework_meta` | `record_this_run` + hourly `framework_runs` | One append per observed task state | Delta, append-only; `v_job_task_runs` keeps latest `(job_run_id, task_run_id)` |
| `pipeline_updates` | `framework_meta` | `framework_runs` | One update of an ingestion pipeline | Delta, merged on `(pipeline_id, update_id)` |
| `load_ledger` | `framework_meta` | each job (`record_load_metrics`) | One job, layer, update, and registered dataflow (FAILED when no progress / missing event log); `job_run_id`, AUTO CDC upserts/deletes | Delta, insert-only, ~7-day hot |
| `load_ledger_archive` / `job_runs_archive` / `job_task_runs_archive` / `pipeline_updates_archive` | `framework_meta` | `framework_optimize` `archive_hot` | Rows moved off hot after 7 days | Delta, forever |
| `ddl_applied` | `framework_meta` | `framework_ddl` / `apply_ddl` | One SQL file hash per successful apply | Delta, insert-only |
| `<pipeline_id>_staged` | none (private) | ingestion pipeline | One source row | Private streaming table or materialized view |
| bronze, `<schema>.<table>` | `<source>_bronze` | ingestion pipeline | One passing row, or one row per key with Auto CDC | Streaming table or materialized view |
| quarantine, `<table>_qrntn` | `<source>_bronze` | ingestion pipeline | One failing row | Streaming table or materialized view |

The registry tables change once per deploy, hot→archive once a day, and hourly `framework_runs` is catch-up. A delivery or curation run writes its own data tables, event log under `framework_events`, one append into `load_ledger` for its layer, and NRT `job_runs` via `record_this_run`. Schemas, volumes, and foreign catalogs exist before a deploy. Until go-live, drop `framework_meta` (and recreate `framework_events` via DDL) to pick up new columns; `CREATE IF NOT EXISTS` will not ALTER existing tables.

### Registry, physical

`asset_registry` is SCD Type 2 on `(object_kind, full_name)`. Each version has `valid_from`, `valid_to`, `is_current`, and `is_deleted`, plus a stable `asset_id` (SHA-256 of source coordinates). A definition has exactly one current version. When a deploy changes it, one `MERGE` sets the old version's `valid_to` and `is_current = false` and inserts the new one, so a retried deploy cannot leave the history half written. When a deploy no longer defines it, the current version is closed and a deletion version (`is_deleted = true`) becomes current. If the definition comes back, the deletion version is closed in turn. Rows are never deleted, and only `valid_to` and `is_current` are ever updated. `ownership_refusals` and `load_ledger` are events and stay insert-only. A version is `(object_kind, full_name, valid_from)`. The definition active at time `t` is the version with `valid_from <= t` and `valid_to` null or later, in `v_asset_registry_history`.

```mermaid
erDiagram
  asset_registry ||--|| asset_registry_current : "latest version"
  asset_registry_current ||--o{ ownership_refusals : "refused object_kind, full_name"
  asset_registry {
    string asset_id
    string object_kind
    string full_name
    string owner_kind
    string owner
    string requested_by
    string delivery
    string parent_full_name
    string relation
    string config_json
    string content_hash
    string source
    string bundle_id
    timestamp recorded_at
    variant config
    timestamp valid_from
    timestamp valid_to
    boolean is_current
    boolean is_deleted
  }
  asset_registry_current {
    string asset_id
    string object_kind
    string full_name
    string owner_kind
    string owner
    string config_json
    variant config
    string content_hash
    string bundle_id
    timestamp recorded_at
  }
  ownership_refusals {
    string object_kind
    string full_name
    string refused_owner_kind
    string refused_owner
    string holder_owner_kind
    string holder_owner
    string failure_reason
    string content_hash
    timestamp recorded_at
  }
```

`asset_registry_current` has every column of `asset_registry` except the four SCD columns: it is the current, non-deleted version of each definition. The diagram shows the keys and the columns most queries use.

| Table | Clustered by |
| --- | --- |
| `asset_registry` | `(owner, object_kind, full_name)` |
| `asset_registry_current` | `(owner, object_kind, full_name)` |
| `ownership_refusals` | `(full_name, recorded_at)` |

Every registry table sets `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`.

`config` is `config_json` parsed to `VARIANT`; `content_hash` is computed over `config_json`, so both are kept.

### Runs, from the Jobs API and the event logs

Run outcomes do not need the system tables. `framework_runs` copies them hourly:

```mermaid
erDiagram
  v_workflows ||--o{ job_runs : "job_name = tags lakeflow_framework_workflow"
  job_runs ||--o{ job_task_runs : "job_run_id = run_id"
  v_workflows ||--o{ pipeline_updates : "job_name = event log name without _event_log"
  job_runs ||--o{ load_ledger : "job_name / run context"
  load_ledger ||--o| v_load_failures : "FAILED or failure_reason"
  job_runs ||--o| job_costs : "job_id, run_id (optional)"
  pipeline_updates ||--o| pipeline_costs : "pipeline_id, update_id (optional)"
  job_runs {
    string job_name
    string job_id
    string run_id
    timestamp started_at
    timestamp ended_at
    string status
    string result_state
    string termination_code
    string failure_reason
    double run_seconds
  }
  job_task_runs {
    string job_run_id
    string task_run_id
    string task_key
    string status
    string failure_reason
  }
  pipeline_updates {
    string job_name
    string pipeline_id
    string update_id
    timestamp started_at
    timestamp ended_at
    string state
    string status
  }
```

- **Job runs.** Append-only (like `load_ledger`): `record_this_run` and hourly catch-up INSERT; `v_job_runs` / `v_job_task_runs` keep the latest `recorded_at` per key — no MERGE under concurrent NRT writers. Catch-up prefers one INSERT SELECT over `system.lakeflow.job_run_timeline` / `job_task_run_timeline` (needs `SELECT` on `system.lakeflow`); without it, Jobs API walk. `status` is `RUNNING`, `SUCCEEDED`, `SKIPPED`, or `FAILED` (`SUCCESS_WITH_FAILURES` → `FAILED` on the API path). System-table freshness is typically up to about an hour.
- **Pipeline updates.** The `pipeline_updates` task reads the `update_progress` events in every `<job>_event_log`. An update with an event in the window is read whole. `state` is the last update state, and `status` is `SUCCEEDED`, `FAILED`, `CANCELED`, or `RUNNING`.
- **Cost, optional.** `system_views.sql` joins `job_runs` and `pipeline_updates` to `system.billing.usage` and `list_prices` as `v_job_costs` and `v_pipeline_costs`. It needs `SELECT` on `system.billing` and runs only when the target sets `system_views` to `true`. `v_observed_lineage` there needs `system.access`.

A run that started and ended between two passes is still listed: the window starts before the latest recorded start. Runs older than the Jobs API keeps (60 days) are only in `job_runs` if `framework_runs` saw them in time.

### Registry, logical

Every box is a row in `asset_registry`, except an ownership refusal which lands in `ownership_refusals`. `parent_full_name` and `relation` are the edges.

```mermaid
erDiagram
  SOURCE_SYSTEM ||--|{ SCHEMA : contains
  SOURCE_SYSTEM ||--|{ WORKFLOW : "one per delivery"
  WORKFLOW |o--o{ PIPELINE : owns
  PIPELINE ||--o{ RUN_STEP : "ownership refusals"
  PIPELINE ||--|| DATAFLOW : contains
  DATAFLOW ||--|{ FLOW : "reads writes rejects_to"
  FLOW ||--o| SECURABLE : "writes or rejects_to"
  PIPELINE ||--|{ LINK : "writes or rejects_to"
  LINK }o--|| SECURABLE : names
  SECURABLE ||--o{ COLUMN : column_of
```

| Kind | `full_name` | Parent | `relation` | What `config` holds |
| --- | --- | --- | --- | --- |
| `source_system` | source system name | none | none | `source.yaml`: domain, owner email, source kind, engine, foreign catalog, raw volume, and each delivery with its schedule or trigger and job name |
| `schema` | `<catalog>.<schema>` | the source system | `contains` | The layers the source writes there, and the tags `registry_sync` sets on the schema |
| `workflow` | `job_name` | none | none | Every `pipeline_id` in that job, every requester, the source system and kind, and the delivery's schedule or trigger |
| `pipeline` | `pipeline_id` | `job_name` when set | `owns` | Canonical spec and compiled rules |
| `dataflow` | `pipeline_id` | the pipeline | `contains` | Raw layer, bronze table, and quarantine table |
| `flow` | `<pipeline_id>.raw`, `.bronze`, `.quarantine` | the dataflow | `reads`, `writes`, or `rejects_to` | Volume path, or table name and layer |
| `streaming_table`, `table`, `materialized_view` | `<catalog>.<schema>.<table>` | the flow name | `writes` or `rejects_to` | Layer, domain, data domain, team, technical owner, classification, PII, `pipeline_id`, job name, and load method |
| `link` | `<pipeline_id>-><table>` | pipeline | `writes` or `rejects_to` | Child table name |
| `column` | `<table>.<column>` | table name | `column_of` | Ordinal, type, description, classification, PII |
| `ownership_refusals` row | the refused pipeline or table | none | none | refused owner, holder, and `failure_reason` |

A file source whose raw layer is a volume still has a `raw` flow. That flow records the volume path and has no securable. A `cloud_files` raw flow has a securable, because its raw layer is a Delta table. A database load has no raw flow. Every row carries `owner_kind`, `owner`, `requested_by`, and `delivery`. A raw table is a `streaming_table` for Auto Loader and a `table` for COPY INTO and CTAS. Bronze and quarantine are a `materialized_view` for CTAS and for a database `snapshot`, and a `streaming_table` otherwise. Bronze written by Auto CDC (dedup, or database `merge`) stays a `streaming_table`.

Each object is hashed on its own document. A job rename versions the workflow, the pipeline, and its tables, because the job name is a table tag. It does not version a column whose own fields did not change. A column hash covers ordinal, data type, description, classification, and PII. The column name is the key, so a rename is a different column.

### Data tables

What each table the pipeline writes holds, beyond the spec's `columns`:

| Table | Extra columns | Rows |
| --- | --- | --- |
| `<pipeline_id>_staged` | `_source_file` and `_ingested_at` (volume files), `_ingested_at` (database), `_rescued_data` (Auto Loader on CSV, delimited, JSON, text), `_dq_failed_rules` | Every source row |
| bronze | The same lineage columns, without `_dq_failed_rules` | Rows with no failed rule; with Auto CDC, the latest row per key by the watermark |
| quarantine | The same lineage columns and `failed_rules` | Rows with at least one failed rule |

Every published table gets a comment, `table_properties` (`quality` = its layer, and `lakeflow_framework.pipeline_id`), and liquid clustering. At deploy, `registry_sync` sets these Unity Catalog tags:

| On | Tags | From |
| --- | --- | --- |
| each schema the source uses | `domain`, `owner` (the source system), `owner_email`, `source_kind`, `layer`, `foreign_catalog`, `engine` | `source.yaml`, and the layers its specs write there |
| each table | `classification`, `domain`, `pii`, `data_domain`, `team`, `technical_owner`, `layer`, `pipeline_id`, `job_name`, `load_method`, `owner`, `requested_by`, `delivery` | the table spec, joined with `source.yaml` |
| each column | `classification`, `pii`, and `description` as the comment | the spec's `columns` |

A tag with no value is not set, and one the spec no longer gives is unset. Tags the framework does not own are left alone. A value is trimmed and cut to 255 characters, within the Unity Catalog limit. A schema or table that does not exist yet is skipped and tagged at the next deploy.

### Metrics, physical

```mermaid
erDiagram
  event_log ||--o{ load_ledger : "record_load_metrics per update and layer"
  load_ledger ||--o| v_loads : "latest per job, layer, update, dataflow"
  load_ledger ||--o| load_ledger_archive : "archive_hot after 7 days"
  load_ledger_archive ||--o| v_loads_90d : "hot union archive"
  pipeline_updates ||--o{ load_ledger : "pipeline_id, update_id"
  load_ledger {
    string job_name
    string layer
    string pipeline_id
    string update_id
    string dataflow
    string target_table
    string quarantine_table
    bigint rows_read
    bigint rows_written
    bigint rows_quarantine
    bigint rows_processed
    string target_status
    string failure_reason
    timestamp recorded_at
  }
```

`job_name` comes from the event log's table name, `<job>_event_log`. `pipeline_id` is the Databricks pipeline id, which joins to `v_pipeline_updates` with `update_id`. `dataflow` is the spec's `pipeline_id`, which joins to the registry. Rule counts land on the ledger from staged `flow_progress` when present.

**Per-load counts.** `v_loads` (or `load_ledger`) is the sole recon SoR for bronze, silver, gold, and product. Each delivery job (`ALL_DONE`) writes one row per registered dataflow, left-joined to that update's event log; missing progress is FAILED so partial delivery failures stay visible. Never MERGE. Filter on `layer` for one medallion layer (`WHERE layer = 'bronze'` for bronze only). `v_loads_90d` unions 7-day hot with forever archive for dashboards. `v_load_failures` joins loads to job/task `failure_reason` for repair.

## Views

```mermaid
flowchart TD
  asset_registry --> v_asset_registry_history
  asset_registry_current --> v_asset_registry_current
  v_asset_registry_current --> v_source_systems
  v_asset_registry_current --> v_schemas
  asset_registry_current --> v_lineage
  asset_registry_current --> v_ownership_conflicts
  asset_registry_current --> v_bronze_tables
  asset_registry_current --> v_metadata_drift
  information_schema[catalog information_schema] --> v_metadata_drift
  v_asset_registry_current --> v_workflows
  v_asset_registry_current --> v_pipelines
  v_asset_registry_current --> v_dataflows
  v_asset_registry_current --> v_flows
  v_asset_registry_current --> v_tables
  v_asset_registry_current --> v_columns
  v_asset_registry_history --> v_column_history
  ownership_refusals --> v_ownership_refusals
  v_workflows --> v_job_status
  v_job_runs --> v_job_status
  v_workflows --> v_job_runs
  job_runs --> v_job_runs
  job_task_runs --> v_job_task_runs
  pipeline_updates --> v_pipeline_updates
  v_workflows --> v_pipeline_updates
  load_ledger --> v_loads
  load_ledger --> v_loads_90d
  load_ledger_archive --> v_loads_90d
  v_loads --> v_load_failures
  v_job_runs --> v_load_failures
  v_job_task_runs --> v_load_failures
  v_job_runs --> v_job_costs
  v_pipeline_updates --> v_pipeline_costs
  asset_registry_current --> v_observed_lineage
  system_tables["system.billing, system.access (optional)"] --> v_job_costs
  system_tables --> v_pipeline_costs
  system_tables --> v_observed_lineage
```

`framework_meta.sql` creates every view except three; `system_views.sql` creates `v_job_costs`, `v_pipeline_costs`, and `v_observed_lineage`, in their own job because they need system table grants. That job runs them only when the target sets `system_views` to `true`. The views read fields from `config` with `config:path`. `v_job_status` is each workflow and its latest run from `v_job_runs` (NRT from `record_this_run`; hourly `framework_runs` is catch-up). `v_ownership_refusals` lists every definition `registry_sync` refused. `v_loads` keeps the latest `load_ledger` row per job, layer, update, and dataflow (retries of `record_load_metrics` may append again). `v_loads_90d` unions hot and archive; `v_load_failures` surfaces what failed and why. Frozen schema: [META_MODEL_REVIEW.md](META_MODEL_REVIEW.md).

`v_asset_registry_history` reads the SCD columns from `asset_registry`; it does not window. Filter `owner`, the first clustering key, to read less. `asset_registry_current` is keyed by `object_kind` and `full_name`: a pipeline or table has one current row. `v_source_systems` and `v_schemas` list the current source systems and the schemas they use, with their tags. `v_metadata_drift` covers schema, table, and column tags, with `securable` saying which.

How the views join their sources:

| View | Joined on |
| --- | --- |
| `v_job_runs`, `v_job_task_runs` | `job_runs.job_name` (the job's `lakeflow_framework_workflow` tag) = `v_workflows.job_name` |
| `v_pipeline_updates` | `pipeline_updates.job_name` (the event log's name) = `v_workflows.job_name` |
| `v_loads` | `load_ledger` partitioned by `job_name`, `layer`, `update_id`, `dataflow`; latest `recorded_at` |
| `v_loads_90d` | hot `load_ledger` union `load_ledger_archive` (QUALIFY latest) |
| `v_load_failures` | loads + job/task runs where status FAILED or `failure_reason` set |
| `v_job_costs` | `system.billing.usage` `usage_metadata.job_id` and `job_run_id` = `job_runs.job_id` and `run_id` |
| `v_pipeline_costs` | `usage_metadata.dlt_pipeline_id` and `dlt_update_id` = `pipeline_updates.pipeline_id` and `update_id` |
| `v_observed_lineage` | `table_lineage.target_table_full_name` = the dataflow's bronze or quarantine table |
| `v_metadata_drift` | `information_schema` schema, table, and column names = the registry names, lower-cased |

## Design choices

**The Unity Catalog volume is the raw layer for file kinds.** `sftp_files` and `sharepoint_files` keep the files as delivered and have no raw Delta table. A database has no raw layer. `cloud_files` writes a raw table first. Its path may be `abfss://`, `s3://`, or a `/Volumes/<catalog>/<schema>/<volume>` path. Quarantine and bronze are always built, and both are filtered by the quality rules.

**The source system owns bronze.** Many teams can add specs under one source. The registry records `requested_by` for the team that asked. `registry_sync` refuses a second source system for the same table. Silver, gold, and data products are owned by a project.

**A delivery is the pipeline.** Every table in a delivery loads in one Lakeflow pipeline and one job. A second schedule is a second delivery in that source's `source.yaml`. A new table in an existing delivery is a spec file and nothing else. Cross-table waits belong to curation, not to bronze.

**Each source is read once.** A pipeline cannot send one read to two tables, and a temporary view runs again for every reader, so a private staging table holds the one read. Bronze holds only clean rows; quarantine keeps the failing rows with `failed_rules`.

**Facts live once.** The source's kind, raw volume, and foreign catalog are in `source.yaml`. A table spec holds only the table. `compose_document` joins them, so a new table cannot disagree with its source.

**Everything operational is generated from the specs.** Pipelines, jobs, and maps come from one model, so the map is the deployed state. Generated files are marked, deleted when unneeded, and checked in CI.

**Load methods are strategies** with one contract: name, supported formats, the raw relation, options, and batch SQL. The pipeline template reads the strategy. It does not subclass a reader hierarchy.

**Quality rules are functions from config to SQL.** The template does not know how a not-null rule was produced.

**The registry records intent; the platform records what happened.** Definitions and owners are in the registry, written once per deploy. Row counts and pipeline update states come from the event logs, job runs from the Jobs API, and, where the grants exist, cost from `system.billing` and observed lineage from `system.access`. The views join them.

**System tables are preferred, not required, for catch-up.** Hourly job-run catch-up prefers `system.lakeflow` and falls back to the Jobs API without the grant. Cost and observed lineage still need `system.billing` / `system.access` and stay behind `system_views`. NRT status never waits on system tables (`record_this_run`).

**Unity Catalog holds the governance metadata.** The spec and `source.yaml` are the source; `registry_sync` writes them to Unity Catalog at every deploy and changes only what differs. `v_metadata_drift` shows anything that disagrees.

**Definitions are SCD Type 2; events are not.** A definition has a life: it is created, changed, and removed, and `asset_registry` keeps every state with when it held. A refusal or a load-metrics append happens once, so `ownership_refusals` and `load_ledger` only append.

**One bundle retires one owner kind.** `registry_sync` retires every `source_system`-owned object in the catalog that `framework_ingestion` no longer defines. That is right while `framework_ingestion` is the only bundle that registers source systems in a catalog. A second bundle that registers source systems into the same catalog, or two developers deploying different branches into one dev catalog, would retire each other's definitions.

**Auto Loader options are a fixed map.** A format allow-list, `max_files_per_trigger` (1000 when the spec omits it), JSON `multiline` only when the spec sets it, a required CSV delimiter, Excel `header_rows` of 0 or 1, and a required Excel `data_address`. The pipeline owns the checkpoint and `cloudFiles.schemaLocation`. CSV, delimited text, and JSON get `cloudFiles.schemaHints` from `columns`, and `source.file_events: true` sets `cloudFiles.useManagedFileEvents`.

Lakeflow Connect is how SharePoint, and later other managed sources, will reach the raw volume or bronze. See the [roadmap](ROADMAP.md).

## Behaviour that still needs a workspace

See the list in [END_TO_END.md](END_TO_END.md#checked-on-a-real-workspace).
