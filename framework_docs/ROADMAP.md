# Roadmap

What is built, what phase 1 covers, and what has not been verified.

## Built (MVP)

| Area | State |
| --- | --- |
| Platform bundle | Registry DDL in `framework_bundle/sql/framework_meta.sql`, daily `OPTIMIZE` and table health, and the `framework_metrics` pipeline over every ingestion event log. Ownership columns and checks. |
| Ingestion kinds | `sftp_files` and `sharepoint_files` (raw volume to bronze), `database` (PostgreSQL, Azure SQL, SQL Server, DB2 through a foreign catalog, to bronze), and the original `cloud_files` path, which is frozen: the code is kept but validation rejects `autoloader`, `copy_into`, and `ctas` until they run end to end on serverless |
| Config-driven output | One pipeline and one job per delivery, plus `SOURCE_MAP.md`, generated from `source.yaml` and the table specs. The job is one `refresh_pipeline` task; the pipeline reads its deployed specs from `lff.specs`. |
| Registry | Source system, schema, workflow, pipeline, dataflow, flow, securable, link, and column definitions, kept as SCD Type 2 with deletion versions for removed specs. Written once per deploy by `registry_sync`. Views for definitions, lineage, and history. Load metrics in `table_load_metrics`. Job, task, and pipeline runs from the Jobs API and the event logs, hourly, with no system table grants. Cost and observed lineage from the system tables when a target turns `system_views` on. |
| Unity Catalog metadata | `registry_sync` at each deploy sets schema tags from `source.yaml`, table tags from the spec and its source, column tags, and column comments, changing only what differs. Generated jobs carry the source's tags. `v_metadata_drift` and `v_observed_lineage` compare the registry with Unity Catalog and `system.access`. |
| Ownership | Bronze is owned by its source system. `requested_by` and `delivery` are recorded. `registry_sync` refuses a second owner. `v_ownership_conflicts` is empty when the rules hold. |
| Curation structure | `framework_curation/<domain>/<project>/project.yaml` with silver, gold, and data products, reference and ownership checks, `PROJECT_MAP.md` and `PROJECTS_INDEX.md` |
| Scaffolding | `new-source`, `new-table`, `new-project` |
| Checks | Package tests (no Spark), pre-commit lint, `generate:check`, `projects:check`, and the GitHub `checks` workflow |

## Phase 1

**1. Lakeflow Connect ingestions.** Today the framework reads files from the raw volume and does not put them there. Phase 1 adds the managed ingestions that do: SharePoint first, then the others in scope, and SFTP with a copy job or a connector. The target is the raw volume for file kinds and bronze for managed database ingestion. The `fetch` setting in `source.yaml` is rejected until this exists; it is where a delivery will name its ingestion. Each connector should be a load strategy and a `source.yaml` rule set, as in [MAINTAINERS.md](MAINTAINERS.md#adding-a-source-kind).

**2. Silver, gold, and data products.** The structure exists: projects under `framework_curation`, references, ownership, deadlines, and maps. What does not exist is the config for a transform and a builder:

- how a silver table states its transform (SQL, columns, keys, history);
- how a gold table or data product does;
- the pipeline and job generated per project, with triggers on the tables it waits for (`table_update` with `ALL_UPDATED`), and what happens at a `deadline`;
- the registry rows for project-owned tables (`owner_kind` is already `project`).

Start with one silver and one gold example and review the shape before building.

**3. Deliberately outside the framework (owned by each domain).** Schemas and the raw volume exist before a deploy. The framework does not create them. The catalog, the foreign catalogs, and the external locations also exist already.

## Still open from the framework review

- **Production settings.** Every bundle's `prod` target still uses the `aal_dev_fdh` catalog, and no target sets `run_as`. Set the prod catalog and a service principal before a production deploy.
- **Large database tables.** An `incremental_column` would let a `database` load read only rows newer than the last load instead of the whole table. Not decided.
- **Archiving loaded files.** Auto Loader's `cleanSource` could move or delete files after they load. Not decided; the raw volume currently keeps every file.

## Next after phase 1

- At go-live, stop dropping `framework_meta` for table changes. From then on, each change is an `ALTER` in `framework_meta.sql` that is safe to repeat, as [MAINTAINERS.md](MAINTAINERS.md) describes.
- Move every job back to `performance_target: STANDARD` once loads run unattended. Set `PERFORMANCE_TARGET` in `ingestion.py`, regenerate, and change `framework_ops.job.yml` and `registry_sync.job.yml`. `PERFORMANCE_OPTIMIZED` is used for now because it starts in seconds instead of minutes.
- Optional split of a busy delivery, so one bad spec cannot fail the other teams' tables in it.
- Coverage reporting in CI.
- A wheel published to a volume or a feed, so project bundles stop rebuilding it on every deploy.
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
