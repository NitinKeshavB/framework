# Lakeflow framework

Config-driven ingestion and a metadata registry for Databricks Lakeflow. Teams add YAML, run one command, and the framework produces the pipelines, jobs, and a map. Source ingestion is shared by a domain. Silver, gold, and data products belong to projects.

## How it fits together

```
source system ──delivery──▶ bronze ──▶ silver / gold / product
 (framework_ingestion)                 (framework_curation)
        every definition and run is recorded in framework_meta (framework_bundle)
```

| Folder | Role | Owner |
| --- | --- | --- |
| `framework_lff_lib` | The wheel: config validation, pipeline graph, generators, registry rows. Public names are `build_pipeline`, `validate_configs`, and `LoadMethod`. | Platform team |
| `framework_bundle` | The deployed framework: the wheel, frozen `framework_meta` DDL, hourly `framework_runs` catch-up, and daily `OPTIMIZE` / archive | Platform team |
| `framework_ingestion` | Source to bronze. Specs, and the pipelines and jobs generated from them. | Domain, per source system |
| `framework_curation` | One bundle per project for silver, gold, and data products, with ownership checks and maps | One project each |
| `framework_examples/ingestion` | Specs for each ingestion kind: a raw volume, SharePoint, a database, and cloud files | None |
| `framework_examples/consumer` | The original single-bundle example. Reference only. | None |
| `framework_tests` | Package tests. They do not start Spark. | Platform team |
| `framework_docs` | Contract, end-to-end path, architecture, maintainers, roadmap | Platform team |

## What each source kind does

| Kind | Data arrives as | Raw layer | Bronze load |
| --- | --- | --- | --- |
| `sftp_files`, `sharepoint_files` | Many files | A Unity Catalog volume of files as delivered | Auto Loader reads the volume into bronze |
| `database` | Tables in PostgreSQL, Azure SQL, SQL Server, or DB2 | None. A foreign catalog exposes the database | Each table loads into bronze: `snapshot` or `merge` |
| `cloud_files` | Files on ADLS or S3 | A raw table | Auto Loader, COPY INTO, or CTAS, then bronze. Not in the MVP: validation rejects these methods |

Rows that fail a quality rule go to a quarantine table. Getting SFTP and SharePoint files into the raw volume is not built; it is the first phase 1 item ([ROADMAP](framework_docs/ROADMAP.md)).

## Who owns what

- A **source system** belongs to a domain. Its bronze tables are owned by the source system, never by the team that asked for them. The registry records `requested_by` beside the owner.
- A **project** owns its silver, gold, and data-product tables. A table name is owned once; another project reads it as `<project>.silver.<name>` or `<project>.product.<name>`.
- `registry_sync` refuses a definition for a table another owner holds and records a failed `ownership_check` step. `v_ownership_conflicts` stays empty.

## Add a table to a source system

```bash
./run.sh new-source finance erp erp.owner@jll.com database nightly   # new source system only
./run.sh new-table finance erp customers billing                      # the project is last; omit it to put the spec next to source.yaml
# edit sources/erp/billing/erp_customers.yaml and replace every TODO
./run.sh generate
```

`generate` validates every source and spec, then writes committed `SOURCE_MAP.md` and gitignored `.build/` artifacts (pipelines, jobs, delivery manifests). Details are in [framework_ingestion/README.md](framework_ingestion/README.md).

## Add a project, silver, gold, or data product

```bash
./run.sh new-project finance billing_reporting reporting.owner@jll.com
# edit framework_curation/finance/billing_reporting/project.yaml
./run.sh projects:generate
```

`projects:generate` checks every reference against the ingestion bundle and the other projects, then writes `PROJECT_MAP.md` and `PROJECTS_INDEX.md`. Details are in [framework_curation/README.md](framework_curation/README.md). The transforms themselves are phase 1.

## Prerequisites before a deploy

- The catalog exists, and you can create objects in it.
- The schemas the specs name exist, for every source system: its raw, bronze, and quarantine schemas.
- For `sftp_files` and `sharepoint_files`, the raw volume exists. The file-arrival trigger needs it when the job is created.
- For `database`, the foreign catalog exists and can read the source tables.
- Something delivers the files into the raw volume.
- You are logged in: `databricks auth login --host <host> --profile <name>`.

## Deploy order

1. `./run.sh bundle:deploy framework_bundle <target> <profile>`. After the upload it runs `framework_ddl`, so the registry tables and views exist when the command returns. It then runs `framework_system_views`, which does nothing unless the target sets `system_views` to `true`; see [cost and observed lineage](framework_docs/END_TO_END.md#cost-and-observed-lineage-optional). Nothing else needs system table grants.
2. `./run.sh bundle:deploy framework_ingestion <target> <profile>`. After the upload it runs `registry_sync`, which writes every deployed spec into the registry and applies the specs' tags and column comments in Unity Catalog. It is the only writer of definitions.
3. `./run.sh bundle:deploy framework_curation/<domain>/<project> <target> <profile>`, once projects have silver and gold resources.

`bundle:validate` and `bundle:deploy` regenerate the maps and resources first, so an invalid spec stops the run before Databricks is contacted. Both bundles use the same catalog per target, because ingestion reads the registry in the platform bundle's catalog. A delivery job refreshes its pipeline (then records load metrics and NRT run status): the pipeline reads its delivery from `lff.manifest`, and a run does not write the registry.

## See what is running

Query `<catalog>.framework_meta`:

| View | What it answers |
| --- | --- |
| `v_asset_registry_current` | Current definitions for every object kind, with owner |
| `v_workflows` | Each delivery job, its pipeline ids, and every requester |
| `v_pipelines` | Source system, requesting team, delivery, domain, data domain, team, owner, and who is mailed |
| `v_dataflows` | Raw path or table, bronze, and quarantine for each table load |
| `v_flows` | Raw, bronze, and quarantine flows, with relation and path or table |
| `v_tables` | Owner, requester, classification, and PII on each table |
| `v_asset_registry_history` | Every version of a source system, schema, workflow, pipeline, flow, table, link, or column, with `valid_from`, `valid_to`, `is_current`, and `is_deleted` (SCD Type 2). Filter it to see the system at a moment. |
| `v_source_systems`, `v_schemas` | Each source system as `source.yaml` declares it, and the schemas it uses with their tags. |
| `v_columns`, `v_column_history` | Columns of raw, bronze, and quarantine: type, description, tags, and every past version |
| `v_lineage` | Links from a pipeline to its tables |
| `v_job_status` | Each job with its latest run: `RUNNING`, `SUCCEEDED`, `FAILED`, or `SKIPPED` (written by the hourly `framework_runs` job, which also records runs that never reached the pipeline) |
| `v_loads`, `v_load_failures` | Per-load recon and failures, written by the pipeline itself once per update (an event hook): rows read, written, upserted, deleted, quarantined, status, and the error. `batch_id` joins a row to its load |
| `v_ownership_conflicts` | Tables claimed by two owners. Empty when the rules hold. |
| `v_ownership_refusals` | Definitions a deploy refused because another owner holds the table or pipeline |
| `v_job_runs`, `v_job_task_runs`, `v_pipeline_updates` | Run and update state, termination code, and durations (NRT + hourly catch-up) |
| `v_job_costs`, `v_pipeline_costs` | DBUs and list cost per run and update. Only when `system_views` is `true` |
| `v_metadata_drift` | Declared tags and column comments that Unity Catalog does not hold, or holds differently |
| `v_observed_lineage` | Sources Unity Catalog saw each bronze and quarantine table read, against the declared source. Only when `system_views` is `true` |

Filter by `owner`. Definitions are SCD Type 2: a changed spec closes its old version, a removed spec gets a deletion version, and nothing is deleted. Events (`run_ledger`, `load_ledger`) are insert-only.

```sql
-- latest status for every job owned by billing
SELECT job_name, status, termination_code, run_id, started_at
FROM aal_dev_fdh.framework_meta.v_job_status
WHERE owner = 'billing';

-- what a delivery job loads
SELECT job_name, pipeline_ids, requesters, delivery
FROM aal_dev_fdh.framework_meta.v_workflows
WHERE owner = 'billing';
```

## Work in this repo

```bash
source venv/bin/activate        # or set PYTHON to a venv's python
./run.sh install                # editable install with the dev tools
./run.sh test:quick             # package tests; no Spark
./run.sh lint                   # pre-commit: Ruff, mypy, hygiene, generate checks
./run.sh generate:check         # also a local pre-commit hook when ingestion paths change
./run.sh projects:check         # also a local pre-commit hook when curation/example paths change
./run.sh clean                  # remove wheels, build output, and caches
./run.sh help                   # every task
```

## Read next

- [framework_docs/README.md](framework_docs/README.md) for every YAML contract: `source.yaml`, table specs, `project.yaml`
- [framework_docs/END_TO_END.md](framework_docs/END_TO_END.md) for the run path, quality rules, and the registry
- [framework_docs/ARCHITECTURE.md](framework_docs/ARCHITECTURE.md) for the flows, the data model of every table the framework writes, the module boundaries, and the design choices
- [framework_docs/META_MODEL_REVIEW.md](framework_docs/META_MODEL_REVIEW.md) for the frozen `framework_meta` schema, NRT vs hourly `framework_runs`, and removed objects
- [framework_docs/MAINTAINERS.md](framework_docs/MAINTAINERS.md) for versioning, deploy, and what not to change
- [framework_docs/ROADMAP.md](framework_docs/ROADMAP.md) for what is built, phase 1, and what is not verified
