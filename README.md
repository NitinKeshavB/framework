# Lakeflow framework

Config-driven ingestion and a metadata registry for Databricks Lakeflow. Teams add YAML, run one command, and the framework produces the pipelines, jobs, and a map. Source ingestion is shared by a domain. Silver and gold belong to projects.

## How it fits together

```
source system ──delivery──▶ bronze ──▶ silver ──▶ gold
 (framework_ingestion)                 (framework_projects)
        every definition and run is recorded in framework_meta (framework_bundle)
```

| Folder | Role | Owner |
| --- | --- | --- |
| `framework_src` | The wheel: config validation, pipeline graph, generators, registry rows. Public names are `build_pipeline`, `validate_configs`, and `LoadMethod`. | Platform team |
| `framework_bundle` | Bundle for the registry: `framework_ddl`, `framework_collector`, and daily `OPTIMIZE` | Platform team |
| `framework_sql` | Registry DDL for `framework_meta` | Platform team |
| `framework_ingestion` | Bundle for source to bronze. A domain owns each source system. Any team can request a table. | Domain, per source system |
| `framework_projects` | One bundle per project for silver and gold, with ownership checks and maps | One project each |
| `framework_examples/consumer` | The original single-bundle example. Reference only. | None |
| `framework_tests` | Package tests. They do not start Spark. | Platform team |
| `framework_docs` | Contract, end-to-end path, architecture, maintainers, roadmap | Platform team |

## What each source kind does

| Kind | Data arrives as | Raw layer | Bronze load |
| --- | --- | --- | --- |
| `sftp_files`, `sharepoint_files` | Many files | A Unity Catalog volume of files as delivered | Auto Loader reads the volume into bronze |
| `database` | Tables in PostgreSQL, Azure SQL, SQL Server, or DB2 | None. A foreign catalog exposes the database | Each table loads into bronze: `snapshot` or `merge` |
| `cloud_files` | Files on ADLS or S3 | A raw table | Auto Loader, COPY INTO, or CTAS, then bronze |

Rows that fail a quality rule go to a quarantine table. Getting SFTP and SharePoint files into the raw volume is not built; it is the first phase 1 item ([ROADMAP](framework_docs/ROADMAP.md)).

## Who owns what

- A **source system** belongs to a domain. Its bronze tables are owned by the source system, never by the team that asked for them. The registry records `requested_by` beside the owner.
- A **project** owns its silver and gold tables. A table name is owned once; another project reads it as `<project>.silver.<name>`.
- The collector refuses a definition for a table another owner holds and records a failed `ownership_check` step. `v_ownership_conflicts` stays empty.

## Add a table to a source system

```bash
./run.sh new-source finance erp erp.owner@jll.com database nightly   # new source system only
./run.sh new-table finance erp customers billing                      # the requesting team is last
# edit sources/finance/erp/erp_customers.yaml and replace every TODO
./run.sh generate
```

`generate` validates every source and spec, then writes the pipelines, jobs, and `SOURCE_MAP.md`. Details are in [framework_ingestion/README.md](framework_ingestion/README.md).

## Add a project, silver, or gold table

```bash
./run.sh new-project finance billing_reporting reporting.owner@jll.com
# edit framework_projects/finance/billing_reporting/project.yaml
./run.sh projects:generate
```

`projects:generate` checks every reference against the ingestion bundle and the other projects, then writes `PROJECT_MAP.md` and `PROJECTS_INDEX.md`. Details are in [framework_projects/README.md](framework_projects/README.md). The silver and gold transforms themselves are phase 1.

## Prerequisites before a deploy

- The catalog exists, and you can create objects in it.
- The schemas the specs name exist, for every source system: its raw, bronze, and quarantine schemas.
- For `sftp_files` and `sharepoint_files`, the raw volume exists. The file-arrival trigger needs it when the job is created.
- For `database`, the foreign catalog exists and can read the source tables.
- Something delivers the files into the raw volume.
- You are logged in: `databricks auth login --host <host> --profile <name>`.

## Deploy order

1. `./run.sh bundle:deploy framework_bundle <target> <profile>`
2. Run the `framework_ddl` job, then start `framework_collector`. The collector is the only writer of the registry tables.
3. `./run.sh bundle:deploy framework_ingestion <target> <profile>`
4. `./run.sh bundle:deploy framework_projects/<domain>/<project> <target> <profile>`, once projects have silver and gold resources.

`bundle:validate` and `bundle:deploy` regenerate the maps and resources first, so an invalid spec stops the run before Databricks is contacted. Both bundles use the same catalog per target, because ingestion reads the registry in the platform bundle's catalog.

## See what is running

Query `<catalog>.framework_meta`:

| View | What it answers |
| --- | --- |
| `v_object_current` | Current pipeline, flow, and table definitions, with owner |
| `v_pipelines` | Source system, requesting team, delivery, domain, data domain, team, owner, and who is mailed |
| `v_tables` | Owner, requester, classification, and PII on each table |
| `v_columns`, `v_column_history` | Bronze columns: type, description, tags, and every past version |
| `v_lineage` | Links from a pipeline to its tables |
| `v_run_current`, `v_process` | Latest step in the last 7 days, and every step of a run |
| `v_recon` | Source and target counts on the success step |
| `v_ownership_conflicts` | Tables claimed by two owners. Empty when the rules hold. |

Filter by `owner` and, for runs, `event_date`. History is insert-only.

## Work in this repo

```bash
source venv/bin/activate        # or set PYTHON to a venv's python
./run.sh install                # editable install with the dev tools
./run.sh test:quick             # package tests; no Spark
./run.sh lint                   # pre-commit: ruff, formatting, file hygiene
./run.sh generate:check         # fail if generated ingestion files are stale
./run.sh projects:check         # fail if project maps are stale
./run.sh clean                  # remove wheels, build output, and caches
./run.sh help                   # every task
```

## Read next

- [framework_docs/README.md](framework_docs/README.md) for every YAML contract: `source.yaml`, table specs, `project.yaml`
- [framework_docs/END_TO_END.md](framework_docs/END_TO_END.md) for the run path, quality rules, and the registry
- [framework_docs/ARCHITECTURE.md](framework_docs/ARCHITECTURE.md) for the package boundaries and design choices
- [framework_docs/MAINTAINERS.md](framework_docs/MAINTAINERS.md) for versioning, deploy, and what not to change
- [framework_docs/ROADMAP.md](framework_docs/ROADMAP.md) for what is built, phase 1, and what is not verified
