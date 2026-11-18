# Roadmap

What is built, what phase 1 covers, and what has not been verified.

## Built (MVP)

| Area | State |
| --- | --- |
| Platform bundle | Registry DDL in `framework_bundle/sql/framework_meta.sql`, daily `OPTIMIZE` / `archive_hot`, and hourly `framework_runs`. Recon SoR is `load_ledger` / `v_loads` from each delivery job. Ownership columns and checks. Serial `table_health` DESCRIBE loop removed — file stats are UC / predictive optimization territory. |
| Ingestion kinds | `sftp_files` and `sharepoint_files` (raw volume to bronze), `database` (PostgreSQL, Azure SQL, SQL Server, DB2 through a foreign catalog, to bronze), and the original `cloud_files` path, which is frozen: the code is kept but validation rejects `autoloader`, `copy_into`, and `ctas` until they run end to end on serverless |
| Config-driven output | One pipeline and one job per delivery, plus committed `SOURCE_MAP.md`. Job/pipeline YAML and per-delivery `manifest.json` land under gitignored `.build/` at validate/deploy. Pipeline reads `lff.manifest` (legacy `lff.specs` still works). Specs support `spec_version`, JSON Schema, `extends` profiles, and `tables:` groups. `PipelineConfig` carries a discriminated `source_options` union (`FileSourceOptions` \| `DatabaseSourceOptions`). |
| Registry | SCD2 `asset_registry` + current snapshot with stable `asset_id` (SHA-256 of source coordinates), informational PK/CHECK constraints, and `ref_*` enum tables. Ownership refusals in `ownership_refusals` (not run_ledger). Load recon: `load_ledger` (`pipeline_id` = spec, `update_pipeline_uuid` = Lakeflow UUID) dual-written to long `load_metric`; `v_loads` / `v_load_metrics` / `v_load_failures`. Job/task runs append-only. Event logs in `framework_events`. `v_metadata_drift` short-circuits while `APPLY_TAGS` is false. `archive_hot` bounds the anti-join by age. |
| Unity Catalog metadata | `registry_sync` at each deploy sets schema tags from `source.yaml`, table tags from the spec and its source, column tags, and column comments, changing only what differs. Generated jobs carry the source's tags. `v_metadata_drift` and `v_observed_lineage` compare the registry with Unity Catalog and `system.access`. |
| Ownership | Bronze is owned by its source system. `requested_by` and `delivery` are recorded. `registry_sync` refuses a second owner. `v_ownership_conflicts` is empty when the rules hold. |
| Curation structure | `framework_curation/<domain>/<project>/project.yaml` with silver, gold, and data products, reference and ownership checks, `PROJECT_MAP.md` and `PROJECTS_INDEX.md` |
| Scaffolding | `new-source`, `new-table`, `new-project` |
| Checks | Package tests (no Spark); pre-commit (Ruff `E,F,I,B,S,UP`, pragmatic mypy, hygiene, local generate checks); GitHub `checks` also runs `lint:ci`, mypy, `test:quick`, and the two generate checks |
| Deploy scale | Staged-view registry lookups; owner-scoped retirement + `bundle_id`; event logs in `framework_events`; per-log `pipeline_updates`; `system.lakeflow` job-run catch-up; per-spec isolation in `build_pipelines`; optional `run_as` per source + constrained rule expressions. Physical split of `framework_ingestion` into per-source bundles is the next ops step when teams need independent deploys. |

## Phase 1

**1. Lakeflow Connect ingestions.** Today the framework reads files from the raw volume and does not put them there. Phase 1 adds the managed ingestions that do: SharePoint first, then the others in scope, and SFTP with a copy job or a connector. The target is the raw volume for file kinds and bronze for managed database ingestion. The `fetch` setting in `source.yaml` is rejected until this exists; it is where a delivery will name its ingestion. Each connector should be a load strategy and a `source.yaml` rule set, as in [MAINTAINERS.md](MAINTAINERS.md#adding-a-source-kind).

**2. Silver, gold, and data products.** The structure exists: projects under `framework_curation`, references, ownership, deadlines, and maps. What does not exist is the config for a transform and a builder:

- how a silver table states its transform (SQL, columns, keys, history);
- how a gold table or data product does;
- the pipeline and job generated per project, with triggers on the tables it waits for (`table_update` with `ALL_UPDATED`), and what happens at a `deadline`;
- the registry rows for project-owned tables (`owner_kind` is already `project`).

Start with one silver and one gold example and review the shape before building.

**3. Deliberately outside the framework (owned by each domain).** Schemas and the raw volume exist before a deploy. The framework does not create them. The catalog, the foreign catalogs, and the external locations also exist already.

## Write-path redesign (after append-only job runs)

**Done (A).** `job_runs` / `job_task_runs` are append-only; views dedupe by `recorded_at`. Removes MERGE conflicts under 1000 concurrent NRT writers. Keep this regardless of B/C.

**Next (B) — one writer, delete bookkeeping tasks.** Drop `record_load_metrics` and `record_this_run` from every delivery job. Run one continuous (or 1–5 min micro-batch) consumer that reads consolidated pipeline events + `system.lakeflow` and appends `load_ledger` / `job_runs` as a single writer. Removes ~2000 serverless cold starts and ~3000 commits per wave. Build toward this once pipeline_events consolidation from §3.3 is in place.

**Optional (C) — Zerobus if per-job push stays.** If B is deferred and jobs must still report, push a few records via Zerobus Ingest instead of a Delta commit per job (~150 ms durable, ~5 s queryable). At-least-once → duplicates OK (views already dedupe). Pre-create UC managed Delta targets; grant MODIFY+SELECT **directly on the table** to the SP (schema inheritance is not enough). Check region availability first.

**Read path (requirement #5).** Materialize leaf definition views (`v_workflows`, `v_pipelines`, `v_tables`, `v_columns`) and hot ops views (`v_job_status`, `v_loads`) as Lakeflow materialized views; keep plain views as the compatibility API. Highest-leverage read change with no API break.

**Control plane (not MVP).** Prototype Lakebase Postgres for registry lookups / concurrent small writes (real PKs, FKs, check constraints) with Delta for history/analytics. Postgres→Delta Lakehouse sync is Beta; DABs `postgres_synced_tables` not ready — drive sync from the CLI for now. Prototype before committing to Delta-only for the control plane.

## Still open from the framework review

- **Production settings.** Every bundle's `prod` target still uses the `aal_dev_fdh` catalog. Set the prod catalog before a production deploy. Set each source's `run_as` (service principal) in `source.yaml` and grant that SP only its raw volume / foreign catalog / bronze schema.
- **system.lakeflow grant.** Hourly `framework_runs` prefers `system.lakeflow`; grant the job identity `SELECT` on it (or keep the Jobs API fallback). Freshness is typically up to ~1 hour — NRT stays on `record_this_run`.
- **Large database tables.** An `incremental_column` would let a `database` load read only rows newer than the last load instead of the whole table. Not decided.
- **Archiving loaded files.** Auto Loader's `cleanSource` could move or delete files after they load. Not decided; the raw volume currently keeps every file.

## Next after phase 1

- Turn Unity Catalog tag and comment assignment back on. Set `APPLY_TAGS = True` in `unity.py` once the deploy identity can assign tags under any tag policies on the framework keys (or those policies are removed). Until then the tags are only in the registry and in `v_metadata_drift`.
- At go-live, stop dropping `framework_meta` for table changes. From then on, each change is an `ALTER` in `framework_meta.sql` that is safe to repeat, as [MAINTAINERS.md](MAINTAINERS.md) describes.
- Generated jobs default to `performance_target: STANDARD` (override per source in `source.yaml`). Keep `PERFORMANCE_OPTIMIZED` only for interactive cold-start work.
- Coverage reporting in CI.
- Publish the framework wheel once to a UC volume (or feed) so sharded bundles stop rebuilding it on every deploy wave.
- Dropping the legacy `project:` spelling for `requested_by` in the original example.

## Not verified on a workspace

Listed in [END_TO_END.md](END_TO_END.md#checked-on-a-real-workspace). The first deploy to a development workspace should confirm them before anything else is built. The ones most likely to need a change are the `pyspark.pipelines` calls for `merge`, the file-arrival trigger on a volume, and `${workspace.file_path}` on serverless.

## Decisions already made

- The raw layer of a file source is a volume of files, not a table.
- A database loads straight to bronze; no raw table.
- The source system owns bronze. The team that asked for a table is recorded, not the owner.
- A delivery is one pipeline and one job. A second schedule is a second delivery in `source.yaml`.
- A table has one owner. Another project or team reads it.
- Specs carry no catalog; jobs pass the environment's.
- Silver, gold, and data-product transforms are phase 1, not MVP.
- Each source is read once, into a private staging table. Bronze holds only clean rows; quarantine keeps the failing rows with `failed_rules`.
- Row counts and pipeline update states come from pipeline event logs, job runs from the Jobs API, and cost, where granted, from the system tables. The registry holds definitions, owners, and job steps.
- Unity Catalog tags are plain tags, and column comments are applied with `ALTER` by `registry_sync` at deploy.
- A delivery job only refreshes its pipeline. Definitions are written at deploy, and run outcomes are copied hourly by `framework_runs`.
