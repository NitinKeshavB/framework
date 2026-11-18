# framework_meta — freeze

This document freezes the metadata schema after removing unused dual-path objects.
Until go-live, recreate the schema on deploy so DDL changes apply (`DROP SCHEMA … CASCADE`).

## Hourly `framework_runs` vs NRT (not a 1-hour metrics lag)

| Writer | When | What | Lag |
| --- | --- | --- | --- |
| Delivery job `record_load_metrics` (`run_if: ALL_DONE`) | End of every delivery run | `load_ledger` → `v_loads` / `v_load_failures` | **Near real time** (same job) |
| Delivery job `record_this_run` | After metrics, every run | `job_runs` (+ tasks when available) → `v_job_status` | **Near real time** |
| Hourly `framework_runs` | Cron `0 15 * * * ?` | Catch-up MERGE into `job_runs` / `job_task_runs` / `pipeline_updates` from Jobs API + event logs | Up to ~1h **only for catch-up** (missed NRT, non-delivery jobs, pipeline_updates fill) |

**Recon and job status for delivery jobs do not wait on the hourly job.** Query `v_loads` and `v_job_status` after the run. Keep `framework_runs` as safety net and for `pipeline_updates`.

## Analysis: removed as unnecessary

| Removed | Why |
| --- | --- |
| `v_process` | Passthrough of `run_ledger`. Use `v_ownership_refusals` or `run_ledger`. |
| `v_run_current` | Passthrough of `run_current`. Use `v_ownership_refusals` / `v_job_status`. |
| `v_bronze_loads` | Alias of `v_loads WHERE layer = 'bronze'`. |
| `load_daily_rollup` | Never written; dashboards use `v_loads_90d` (hot ∪ archive). |
| Ops path for `flow_events` / `table_load_metrics` | Already retired; not created by `framework_meta.sql`. Prefer `load_ledger` / `v_loads`. |

## Kept on purpose (not “duplicate waste”)

| Pair / object | Role |
| --- | --- |
| `asset_registry` + `asset_registry_current` | SCD2 history + current lookup snapshot |
| `run_ledger` + `run_current` | Ownership-refusal history + current pointer |
| `config_json` + `config` | Hash source + VARIANT query shape |
| Hot tables + `*_archive` | 7-day hot; forever archive via `archive_hot` |
| `<job>_event_log` | Pipeline-native events feeding metrics / `pipeline_updates` |

## Parallel load: ~1000 delivery jobs at once

**`asset_registry` / `asset_registry_current` are not on the run path** — only `registry_sync` at deploy writes them. Parallel runs do not contend there.

| Writer at run time | Pattern | Under 1000 parallel jobs |
| --- | --- | --- |
| `record_load_metrics` → `load_ledger` | Append-only INSERT | Designed for fan-out. Concurrent Delta appends scale; expect more small files until daily `OPTIMIZE` / auto-optimize. |
| `record_this_run` → `job_runs` | MERGE on `(job_id, run_id)` | Different runs → different keys (low conflict). Many concurrent MERGEs on one table still queue briefly in Delta commits. |
| `job_task_runs` | Same MERGE pattern | Same as `job_runs`. |
| `<job>_event_log` | One table per delivery | Isolated — no cross-job write contention. |
| Hourly `framework_runs` | Catch-up MERGE | Off the NRT critical path; a burst may make catch-up take longer. |

**Usually hits first (workspace limits, not meta design):** Jobs/Lakeflow pipeline concurrency quotas, serverless capacity, UC API rate limits.

**Guidance:** recon/status meta can absorb high fan-out. Rely on append to `load_ledger` (never multi-job MERGE into it). After large parallel waves, let `framework_optimize` compact. If `job_runs` MERGE latency spikes under extreme parallelism, NRT status can lag slightly while `load_ledger` appends stay fine — watch commit duration.

## Frozen read API (query these)

**Definitions:** `v_asset_registry_current`, `v_asset_registry_history`, `v_source_systems`, `v_schemas`, `v_workflows`, `v_pipelines`, `v_dataflows`, `v_flows`, `v_tables`, `v_columns`, `v_column_history`, `v_lineage`, `v_ownership_conflicts`, `v_ownership_refusals`, `v_metadata_drift`, `v_bronze_tables`

**Runs / recon:** `v_job_status`, `v_job_runs`, `v_job_task_runs`, `v_pipeline_updates`, `v_loads`, `v_loads_90d`, `v_load_failures`

**Optional (system grants):** `v_job_costs`, `v_pipeline_costs`, `v_observed_lineage`

**Physical tables still written:** `asset_registry`, `asset_registry_current`, `run_ledger`, `run_current`, `job_runs`, `job_task_runs`, `pipeline_updates`, `load_ledger`, `table_health`, matching `*_archive`, `ddl_applied`, plus per-job `*_event_log`

## Ops: failure and repair

1. Query `v_load_failures` and/or `v_job_status`.
2. Fix the cause.
3. Jobs UI: **Repair and run failed tasks**.
4. Confirm a new `v_loads` row and SUCCEEDED status.

## Deploy note

```sql
DROP SCHEMA IF EXISTS <catalog>.framework_meta CASCADE;
```

Then `./run.sh bundle:deploy framework_bundle …` so `framework_ddl` recreates the frozen DDL.

## Lucid

- Deploy vs Run: https://lucid.app/lucidchart/3bfb81a4-d831-46b1-9f9f-6950ade7d5dc/edit
- ERD + catalog + team process: https://lucid.app/lucidchart/4713f75e-9ce7-407b-a31c-5444a17b9ec9/edit
