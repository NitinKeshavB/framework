# Architecture

## Layout

Folders at the repository root start with `framework_`.

- `framework_src` is the wheel. Public names are `build_pipeline`, `validate_configs`, and `LoadMethod`.
- `framework_bundle` deploys that wheel and the metadata registry: `sql/framework_meta.sql`, the DDL job, the collector, and daily `OPTIMIZE`.
- `framework_ingestion` is source to bronze: source systems, table specs, and the pipelines and jobs generated from them.
- `framework_curation/<domain>/<project>` is one bundle per project for silver, gold, and data products.
- `framework_examples/consumer` is the original single-bundle example. It uses the `cloud_files` shape and is reference only.
- `framework_tests` runs without Spark.
- `framework_docs` is this documentation.

Three things are kept apart on purpose: the **meta framework** (registry and collector), **source ingestion** (domain-owned, shared), and **curation** (silver, gold, and data products, one project owner each). They deploy separately and have different owners.

## Dependency direction

Each module imports only the ones listed:

- `config` imports nothing from the package. It validates specs and joins a spec with its `source.yaml`.
- `load` and `quality` import `config`.
- `pipeline` imports `config`, `load`, and `quality`.
- `metadata` imports `config` and `quality`.
- `collector` imports `metadata` and `pipeline`.
- `ingestion` imports `config`. `projects` imports `config` and `ingestion`.
- `cli` imports all of them.

`pipeline` calls `config`, `quality`, `load`, and a metadata reader passed in. Load strategies return option dictionaries and SQL strings. They do not import `pyspark` or `pipelines`. `build_pipeline` touches Spark and `pyspark.pipelines` only when the caller passes nothing, which is the generated one-line pipeline file on a cluster. `ingestion` and `projects` read YAML and write files. The CLI validates and writes files without a session.

```mermaid
flowchart LR
  spec[source.yaml and table specs] --> gen[generate]
  gen --> res[pipelines, jobs, SOURCE_MAP]
  spec --> sync[job: sync]
  sync --> files[definition and step files]
  files --> collector[Collector]
  collector --> registry[object_registry and run_ledger]
  registry --> pipeline[build_pipeline]
  pipeline --> bronze[bronze and quarantine]
  bronze --> silver[project silver and gold]
```

## Registry, physical

History is insert-only. A version is `(object_kind, full_name, recorded_at)`. A run step is `(run_id, step_no, status)`. The tables do not declare those as primary keys. A run matches the definition that was active when `owner`, `object_kind`, and `full_name` are equal and `started_at` is inside `[valid_from, valid_to)`.

```mermaid
erDiagram
  object_registry ||--o{ run_ledger : "owner, object_kind, full_name"
  object_registry {
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
    timestamp recorded_at
  }
  run_ledger {
    string run_id
    string parent_run_id
    string object_kind
    string full_name
    string owner_kind
    string owner
    int step_no
    string step_name
    string status
    timestamp started_at
    timestamp ended_at
    string failure_reason
    string recon_json
    timestamp recorded_at
    date event_date
  }
```

`object_registry` is clustered by `(owner, object_kind, full_name, recorded_at)`. `run_ledger` is clustered by `(owner, event_date, full_name, run_id)`.

## Registry, logical

Every box is a row in `object_registry`, except `RUN_STEP`, which is a row in `run_ledger`. `parent_full_name` and `relation` are the edges.

```mermaid
erDiagram
  WORKFLOW |o--o{ PIPELINE : owns
  WORKFLOW ||--o{ RUN_STEP : "job run steps"
  PIPELINE ||--|| DATAFLOW : contains
  DATAFLOW ||--|{ FLOW : "reads writes rejects_to"
  FLOW ||--o| SECURABLE : "writes or rejects_to"
  PIPELINE ||--|{ LINK : "writes or rejects_to"
  LINK }o--|| SECURABLE : names
  SECURABLE ||--o{ COLUMN : column_of
```

| Kind | `full_name` | Parent | `relation` | What `config_json` holds |
| --- | --- | --- | --- | --- |
| `workflow` | `job_name` | none | none | Every `pipeline_id` in that job, and every requester |
| `pipeline` | `pipeline_id` | `job_name` when set | `owns` | Canonical spec and compiled rules |
| `dataflow` | `pipeline_id` | the pipeline | `contains` | Raw layer, bronze table, and quarantine table |
| `flow` | `<pipeline_id>.raw`, `.bronze`, `.quarantine` | the dataflow | `reads`, `writes`, or `rejects_to` | Volume path, or table name and layer |
| `streaming_table`, `table`, `materialized_view` | `<catalog>.<schema>.<table>` | the flow name | `writes` or `rejects_to` | Layer, domain, classification, PII |
| `link` | `<pipeline_id>-><table>` | pipeline | `writes` or `rejects_to` | Child table name |
| `column` | `<table>.<column>` | table name | `column_of` | Ordinal, type, description, classification, PII |
| `run_ledger` step | the job name | none | none | Status `RUNNING` / `SUCCEEDED` / `FAILED`, step name, optional recon |

A file source whose raw layer is a volume still has a `raw` flow. That flow records the volume path and has no securable. A `cloud_files` raw flow has a securable, because its raw layer is a Delta table. A database load has no raw flow. Every row carries `owner_kind`, `owner`, `requested_by`, and `delivery`. A raw table is a `streaming_table` for Auto Loader and a `table` for COPY INTO and CTAS. Bronze and quarantine are a `materialized_view` for CTAS and for a database `snapshot`, and a `streaming_table` otherwise. Bronze written by Auto CDC (dedup, or database `merge`) stays a `streaming_table`.

Each object is hashed on its own document, so a job rename versions the pipeline and the workflow but not a table or column whose own fields did not change. A column hash covers ordinal, data type, description, classification, and PII. The column name is the key, so a rename is a different column. Columns are recorded for every table the pipeline writes: raw (with `_source_file` and `_ingested_at`), bronze, and quarantine (with `failed_rules`). Without a raw table, bronze and quarantine carry the lineage columns the pipeline adds.

## Views

Views do not store a second copy. Current views keep the latest row. History views add `valid_from` (`recorded_at`) and `valid_to` (the next `recorded_at` for that object, or null).

```mermaid
flowchart TD
  object_registry[object_registry]
  run_ledger[run_ledger]
  object_registry --> v_object_current
  object_registry --> v_object_history
  object_registry --> v_lineage
  object_registry --> v_ownership_conflicts
  v_object_current --> v_workflows
  v_object_current --> v_pipelines
  v_object_current --> v_dataflows
  v_object_current --> v_flows
  v_object_current --> v_tables
  v_object_current --> v_columns
  v_object_history --> v_column_history
  run_ledger --> v_process
  run_ledger --> v_run_current
  run_ledger --> v_recon
  v_workflows --> v_job_status
  v_run_current --> v_job_status
```

`v_lineage` is the latest `link` row for each parent, child, and relation. `v_workflows`, `v_pipelines`, `v_dataflows`, `v_flows`, `v_tables`, and `v_columns` read `v_object_current` and extract fields from `config_json`. `v_column_history` is `v_object_history` where `object_kind` is `column`. `v_recon` is the `record_success` step with `recon_json` parsed. `v_run_current` is the latest step in the last 7 days. `v_job_status` joins each workflow to that latest step. `v_ownership_conflicts` lists any name with more than one owner. Job run steps are stored with `object_kind = workflow` and `full_name = job_name`.

## Design choices

**The Unity Catalog volume is the raw layer for file kinds.** `sftp_files` and `sharepoint_files` keep the files as delivered and have no raw Delta table. A database has no raw layer. Only `cloud_files` writes a raw table first. Quarantine and bronze are always built, and both are filtered by the quality rules.

**The source system owns bronze.** Many teams can add specs under one source. The registry records `requested_by` for the team that asked. The collector refuses a second source system for the same table. Two teams under the same source share that owner on purpose. Silver, gold, and data products are owned by a project.

**A delivery is the pipeline.** Every table in a delivery loads in one Lakeflow pipeline and one job. A second schedule is a second delivery in that source's `source.yaml`. A project folder does not declare a pipeline. A new table in an existing delivery is a spec file and nothing else. Cross-table waits belong to curation, not to bronze.

**Facts live once.** The source's kind, raw volume, and foreign catalog are in `source.yaml`. A table spec holds only the table. `compose_document` joins them, so a new table cannot disagree with its source.

**Everything operational is generated from the specs.** Pipelines, jobs, and maps come from one model, so the map is the deployed state. Generated files are marked, deleted when unneeded, and checked in CI.

**Load methods are strategies** with one contract: name, supported formats, the raw relation, options, and batch SQL. The pipeline template reads the strategy. It does not subclass a reader hierarchy.

**Quality rules are functions from config to SQL.** The template does not know how a not-null rule was produced.

**The registry is two tables.** Attributes and relationships are columns on `object_registry`. Run steps, failure text, and reconciliation are rows in `run_ledger`.

**Auto Loader options are a fixed map.** A format allow-list, a positive `max_files_per_trigger`, a required CSV delimiter, Excel `header_rows` of 0 or 1, and a required Excel `data_address`. The pipeline owns the checkpoint and `cloudFiles.schemaLocation`.

Lakeflow Connect is how SharePoint, and later other managed sources, will reach the raw volume or bronze. See the [roadmap](ROADMAP.md).

## Behaviour that still needs a workspace

See the list in [END_TO_END.md](END_TO_END.md). Where the code assumes a syntax, the module states the assumption by using the current Lakeflow form, and the docs list the check.
