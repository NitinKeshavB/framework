# Source ingestion bundle

Domain-owned ingestion into bronze. Projects do not own anything here. Any team can ask for a file or a table by adding a spec. Silver, gold, and data products belong to the project bundles in [framework_curation](../framework_curation/README.md).

## Three bundles

| Bundle | Holds | Owner |
| --- | --- | --- |
| `framework_bundle` | The registry (`framework_meta`), the collector, and `OPTIMIZE` | Platform team |
| `framework_ingestion` (this one) | Source to bronze | Domain, per source system |
| `framework_curation/<domain>/<project>` | Silver, gold, and data products | One project each |

Deploy order: `framework_bundle`, then run its `framework_ddl` job and start `framework_collector`, then this bundle, then the projects.

## Source kinds

The kind is set once in the source system's `source.yaml`. Every table in that source inherits it.

| Kind | Data arrives as | Raw layer | Bronze load |
| --- | --- | --- | --- |
| `sftp_files` | Many files | A Unity Catalog volume (`raw_volume`) holding the files as delivered | Auto Loader reads the volume into bronze (`volume_files`) |
| `sharepoint_files` | Many files | The same raw volume | The same |
| `database` | Tables in PostgreSQL, Azure SQL, SQL Server, or DB2 | None. The source is a foreign catalog | Each table is read into bronze (`database_table`), `snapshot` or `merge` |
| `cloud_files` | Files on ADLS or S3 | A raw table | Auto Loader, COPY INTO, or CTAS into the raw table, then bronze. This is the original path |

Rows that fail a quality rule go to a quarantine table beside bronze.

**Files:** the framework loads from the raw volume. It does not copy files from SFTP or SharePoint into the volume. Another process has to do that for now; see "Getting files into the raw volume" below.

**Databases:** the database is exposed through Lakehouse Federation as `foreign_catalog`. Every table in a delivery loads in the same run. `snapshot` replaces bronze with the source table each run. `merge` keeps bronze in step with the source on `primary_keys`.

## Layout

```
framework_ingestion/
  databricks.yml
  sources/<source_system>/                 you edit these
    source.yaml                            kind, owner, raw volume or foreign catalog, deliveries
    <source_system>_<table>.yaml           a spec with no project
    <project>/
      <source_system>_<table>.yaml         a spec that project is bringing in
  src/                                     entry points the jobs call
  SOURCE_MAP.md                            generated
  pipelines/<source_system>_<delivery>.py  generated
  resources/<source_system>_<delivery>.pipeline.yml and .job.yml   generated
```

The first folder is the source system. The child folder is the project bringing the table in, and it matches `requested_by`. When there is no project, paste the spec next to `source.yaml` and omit `requested_by`. A table is registered once. A second project that needs the same table reads the existing bronze table and does not add a copy.

## What a spec holds

A spec holds only what is true of the table. The kind, raw volume, and foreign catalog come from the source's `source.yaml`.

```yaml
# a file table                          # a database table
source:                                 source:
  raw_path: invoices/                     table: dbo.customers
  format: csv                             mode: merge
  delimiter: ","
```

The spec's `raw_to_bronze` section holds the bronze table, keys, columns, and quality rules. Its `quarantine` section holds the reject table. Specs do not name a catalog. Each job passes the environment's catalog.

## One delivery, one pipeline, one job

A delivery is how a source's data arrives: a schedule, or for file kinds a file-arrival trigger on the raw volume. Every table in a delivery, whichever team asked for it, loads in the same Lakeflow pipeline and the same job. The job syncs the specs into the registry, waits until they are visible, refreshes the pipeline, and records the result. Different cadences from one source are separate deliveries.

## Add a table to a source system

1. New source system only: `./run.sh new-source <domain> <source_system> <owner-email> [kind] [delivery]`. It creates `source.yaml` for that kind.
2. `./run.sh new-table <domain> <source_system> <table> <requested-by> [delivery]`. It creates the spec from a template for that kind. The `delivery` argument is only needed when the source has several.
3. Edit the new file and replace every `TODO`. `generate` refuses files that still have one.
4. Run `./run.sh generate`.

That is all. The generator validates every source and spec, then rewrites the map, the pipeline files, and the jobs. Do not edit generated files. Their first line says so, and the next `generate` overwrites them. It deletes generated files that no source asks for any more, and never touches a file without that first line.

## The map

`SOURCE_MAP.md` shows who requested what, each source's raw volume or foreign catalog, its deliveries, every table with where it reads and its bronze target, and a diagram. It comes from the same model as the jobs, so it describes what is deployed.

`./run.sh bundle:validate` and `bundle:deploy` run `generate` first, so the map and the resources are rewritten before the bundle is checked. They then check that every project still finds the bronze tables it reads. An invalid spec stops the run before Databricks is contacted. `./run.sh generate:check` changes nothing and fails if the generated files are out of date, for CI.

## Deploy

```bash
./run.sh bundle:validate framework_ingestion dev <profile>
./run.sh bundle:deploy framework_ingestion dev <profile>
```

## Prerequisites

The framework does not create these. The domain makes sure they exist before a deploy:

- The catalog, and permission to create tables and views in it.
- For every source system: the schemas its specs name (raw, bronze, quarantine). Schemas default to `<source_system>_<layer>`.
- For file kinds: the raw volume named in `raw_volume`. The file-arrival trigger needs it when the job is created.
- For `database`: the foreign catalog, with read access to the source tables.
- Something that delivers the files into the raw volume (see below).

## Ownership and the registry

Bronze is owned by the **source system**. The registry stores `owner_kind = source_system` and `owner = <source system>` on every row, with `requested_by` (the team that asked for the table) and `delivery` beside it. A table has one owner, and the collector refuses a second owner's registration and records a failed `ownership_check` step. The team that asked for a table never owns it.

Each delivery syncs a workflow, one pipeline and dataflow per table, the layer flows, securables, links, and columns. Job run steps use `object_kind = workflow`. Query status with `v_job_status`, definitions with `v_workflows` / `v_pipelines` / `v_dataflows` / `v_flows`, and filter by `owner`. The full view list is in the [root README](../README.md#see-what-is-running).

`requested_by` is the project folder. Omit it only when the spec sits next to `source.yaml`. A spec that says `project:` instead of `requested_by:` is rejected.

## Getting files into the raw volume

This bundle reads from the raw volume. It does not copy files from SFTP or SharePoint into it, so something else must, such as Lakeflow Connect, ADF, or a copy job. A `fetch` setting in `source.yaml` is rejected until that step exists. Building the Lakeflow Connect ingestions is the first item of [phase 1](../framework_docs/ROADMAP.md).

## Known gaps

- Nothing here has run on a workspace. Untested there: the file-arrival trigger on a volume, `${workspace.file_path}` spec paths on serverless, the `pyspark.pipelines` calls in `build_pipeline` (including `create_auto_cdc_from_snapshot_flow` for `merge`), reading a streaming view by name, and the serverless environment version.
- `fixed_width` and `yaml` files are not supported from a raw volume yet.
- A delivery's tables share one pipeline, so one bad spec can fail the others. Splitting a busy delivery is not built.
- The sample specs are for illustration. `framework_examples/consumer` registers `billing_invoices` too, so do not deploy both to the same catalog.
