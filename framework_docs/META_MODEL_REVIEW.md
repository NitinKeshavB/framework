# framework_meta — freeze

This document freezes the metadata schema after removing unused dual-path objects.
Until go-live, recreate the schema on deploy so DDL changes apply (`DROP SCHEMA … CASCADE`).

## Who writes what, and when

| Writer | When | What | Lag |
| --- | --- | --- | --- |
| The pipeline's event hook (`pipeline_metrics`) | Once per update, when every flow has finished | `load_ledger`, `ledger_detail` → `v_loads` / `v_load_failures` | **Near real time** (inside the pipeline; no task after it) |
| `framework_backfill` | When someone runs it | `load_ledger` + `ledger_detail`, `load_kind = 'backfill'` | Immediate |
| Hourly `framework_runs` | Cron `0 15 * * * ?` | `job_runs` / `job_task_runs` / `pipeline_updates` from `system.lakeflow` or the Jobs API and event logs, and the `load_ledger` row of an update whose hook was cut off at shutdown | Up to ~1h |

**Recon for delivery jobs does not wait on the hourly job.** Query `v_loads` after the run. Job run status (`v_job_status`) is as fresh as the last `framework_runs`. The recording tasks `record_load_metrics` and `record_this_run` no longer exist.

## Analysis: removed as unnecessary

| Removed | Why |
| --- | --- |
| `run_ledger` / `run_current` | Vestigial step tables used only for ownership refusals. Replaced by `ownership_refusals`. |
| `v_process` | Passthrough of `run_ledger`. Use `v_ownership_refusals`. |
| `v_run_current` | Passthrough of `run_current`. Use `v_ownership_refusals` / `v_job_status`. |
| `v_bronze_loads` | Alias of `v_loads WHERE layer = 'bronze'`. |
| `v_bronze_tables` | Redundant discovery view; use `v_tables WHERE layer = 'bronze'`. |
| `v_loads_90d` | Folded into `v_loads` (hot ∪ archive). |
| `load_metric` | Long-shape dual-write of ledger counts; retired. Wide `load_ledger` is enough. |
| `file_ledger` | Renamed to `ledger_detail` (`detail_kind=file|input`). |
| `load_daily_rollup` | Never written; dashboards use `v_loads` (hot ∪ archive). |
| Ops path for `flow_events` / `table_load_metrics` | Already retired; not created by `framework_meta.sql`. Prefer `load_ledger` / `v_loads`. |

## Kept on purpose (not “duplicate waste”)

| Pair / object | Role |
| --- | --- |
| `asset_registry` + `asset_registry_current` | SCD2 history + current lookup snapshot |
| `ownership_refusals` | Ownership-refusal history (replaced vestigial `run_ledger` / `run_current`) |
| `config_json` + `config` | Hash source + VARIANT query shape |
| Hot tables + `*_archive` | 90-day hot; forever archive via `archive_hot` |
| `<job>_event_log` | Pipeline-native events feeding metrics / `pipeline_updates` |

## Deploy scaling (registry_sync + event logs)

- Lookups join staged temp views (`_framework_lookup_keys`, `_framework_owner_names`) — do not rebuild `full_name IN (...)` for every row in the bundle.
- Retirement is `owner_kind` **and** `owner IN (this bundle's source systems)` so shards do not delete each other; rows carry `bundle_id`.
- Event logs are in `{catalog}.framework_events`. Hourly `framework_runs` merges **one event log at a time** (no 1000-way `UNION ALL`).

## Parallel load: ~1000 delivery jobs at once

**`asset_registry` / `asset_registry_current` are not on the run path** — only `registry_sync` at deploy writes them. Parallel runs do not contend there.

| Writer at run time | Pattern | Under 1000 parallel jobs |
| --- | --- | --- |
| The event hook → `load_ledger`, `ledger_detail` | Append-only writes, three at once | Designed for fan-out. Concurrent Delta appends scale; expect more small files until daily `OPTIMIZE` / auto-optimize. `ledger_detail` adds one row per file, so a run of many files writes many rows in one append. |
| `job_runs`, `job_task_runs` | Append-only INSERT by `framework_runs` | One writer, hourly; views keep the latest row per key. |
| `<job>_event_log` | One table per delivery | Isolated — no cross-job write contention. |
| Hourly `framework_runs` | Catch-up | Off the load path; a burst may make catch-up take longer. |

**Usually hits first (workspace limits, not meta design):** Jobs/Lakeflow pipeline concurrency quotas, serverless capacity, UC API rate limits.

**Guidance:** recon meta can absorb high fan-out. Rely on append to `load_ledger` and `ledger_detail` (never multi-job MERGE into them). After large parallel waves, let `framework_optimize` compact.

## Frozen read API (query these)

**Definitions:** `v_asset_registry_current`, `v_asset_registry_history`, `v_source_systems`, `v_schemas`, `v_workflows`, `v_pipelines`, `v_dataflows`, `v_flows`, `v_tables`, `v_columns`, `v_column_history`, `v_rules`, `v_rule_history`, `v_lineage`, `v_ownership_conflicts`, `v_ownership_refusals`, `v_metadata_drift`

**Runs / recon:** `v_job_status`, `v_job_runs`, `v_job_task_runs`, `v_pipeline_updates`, `v_loads`, `v_ledger_detail`, `v_load_failures`

**Optional (system grants):** `v_job_costs`, `v_pipeline_costs`, `v_observed_lineage`

**Physical tables still written:** `asset_registry`, `asset_registry_current` (with stable `asset_id`), `ownership_refusals`, `job_runs`, `job_task_runs`, `pipeline_updates`, `load_ledger` (lean recon: `dataflow` = spec id, `update_pipeline_uuid` = Lakeflow UUID; `source_kind`, `run_as`, `load_kind`, `load_date`, counts, `batch_id`; owner/quarantine/expectations enriched in `v_loads`), `ledger_detail` (`detail_kind=file|input`), `flow_checkpoint` (append-only flow watermark; `v_flow_checkpoint` = latest SUCCEEDED; replaces `curated_progress` / `silver_progress`), matching `*_archive` (`PARTITIONED BY archive_quarter`), `ref_*` enum tables, `ddl_applied`, plus per-job `*_event_log` under `framework_events`. Retired: `load_metric`, `file_ledger` (renamed to `ledger_detail`), `silver_progress` / `curated_progress` (renamed to `flow_checkpoint`), `v_loads_90d`, `v_bronze_tables`, `table_health`, `run_ledger`, `run_current`. Append-only facts use views for latest-per-key.

## Ops: failure and repair

1. Query `v_load_failures` and/or `v_job_status`. A job can be green while its load is `FAILED` (no file, or an empty one): query `v_loads` for `target_status = 'FAILED'`, and `ledger_detail` for the file.
2. Fix the cause.
3. A failed run: Jobs UI **Repair and run failed tasks**. A file that was read is not read again, so a blocked or empty file is reloaded with `./run.sh backfill` (`mode=files` or `mode=failed`).
4. Confirm a new `v_loads` row and SUCCEEDED status.

## Deploy note

```sql
DROP SCHEMA IF EXISTS <catalog>.framework_meta CASCADE;
```

Then `./run.sh bundle:deploy framework_bundle …` so `framework_ddl` recreates the frozen DDL.

## Lucid

- Deploy vs Run (7 tabs — Deploy, Run + event-hook ledgers, What writes what, Ingestion kinds, Quarantine, Backfill, Future): https://lucid.app/lucidchart/3bfb81a4-d831-46b1-9f9f-6950ade7d5dc/edit
- ERD + catalog + team process + ingestion features (8 tabs — ERD, catalog NOW, Team process, kinds, quarantine, backfill, metadata via configs, Future Connect/SFTP/SharePoint): https://lucid.app/lucidchart/4713f75e-9ce7-407b-a31c-5444a17b9ec9/edit
