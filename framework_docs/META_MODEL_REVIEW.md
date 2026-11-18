# framework_meta model review — findings and target

Scannable register for the metadata cleanup implemented with central `load_ledger` recon.

## Summary

| Severity | Finding | Action taken |
| --- | --- | --- |
| High | Dual metrics paths | Retired `framework_metrics` / `flow_events` / `table_load_metrics` from the bundle. SoR = `v_loads` |
| High | Metrics skipped on pipeline failure | `run_if: ALL_DONE` + FAILED stub rows when no `flow_progress` |
| High | Job status up to 1h lag | `record_this_run` after metrics for NRT `job_runs` |
| Medium | No failure message in run tables | Added `failure_reason` on job/task runs and load ledger; `v_load_failures` |
| Medium | Unbounded hot facts | 7-day hot + forever archive (`archive_hot`) + `v_loads_90d` |
| Low | Thin passthrough views | Marked `v_process`, `v_run_current`, `v_bronze_loads` deprecated |

## What not to do

- Do not MERGE into `load_ledger` from many delivery jobs.
- Do not purge archive tables.
- Do not use Delta time travel as the archive.
- Do not expect mid-flow seek inside one failed pipeline update — Repair + Auto Loader checkpoint is the resume model.

## Ops: failure and repair

1. Query `framework_meta.v_load_failures` (and/or `v_job_status`).
2. Fix the cause (data, grants, schema, volume).
3. In Databricks Jobs UI: **Repair and run failed tasks** (prefer over a brand-new Run when catching up the same attempt).
4. Confirm a new `v_loads` row and SUCCEEDED status.

## Deploy note

Until go-live, drop `framework_meta` (or recreate) so `CREATE TABLE IF NOT EXISTS` picks up `failure_reason` and archive tables. After go-live, add ALTERs instead.

## Lucid

- Process: https://lucid.app/lucidchart/3bfb81a4-d831-46b1-9f9f-6950ade7d5dc/edit
- ERD: https://lucid.app/lucidchart/4713f75e-9ce7-407b-a31c-5444a17b9ec9/edit
