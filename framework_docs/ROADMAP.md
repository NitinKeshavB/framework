# Roadmap

What is built, what phase 1 covers, and what has not been verified.

## Built (MVP)

| Area | State |
| --- | --- |
| Platform bundle | Registry DDL in `framework_bundle/sql/framework_meta.sql`, collector, daily `OPTIMIZE`. Ownership columns and checks. |
| Ingestion kinds | `sftp_files` and `sharepoint_files` (raw volume to bronze), `database` (PostgreSQL, Azure SQL, SQL Server, DB2 through a foreign catalog, to bronze), and the original `cloud_files` path |
| Config-driven output | One pipeline and one job per delivery, plus `SOURCE_MAP.md`, generated from `source.yaml` and the table specs |
| Registry | Workflow, pipeline, dataflow, flow, securable, link, and column definitions. Job run steps with `RUNNING` / `SUCCEEDED` / `FAILED`. Views for definitions, lineage, history, recon, and `v_job_status`. |
| Ownership | Bronze is owned by its source system. `requested_by` and `delivery` are recorded. The collector refuses a second owner. `v_ownership_conflicts` is empty when the rules hold. |
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

## Next after phase 1

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
