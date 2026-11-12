# YAML contracts

The repository overview is [../README.md](../README.md). This page is every file a team writes. Generated files are never edited.

| File | Who writes it | Where |
| --- | --- | --- |
| `source.yaml` | The source system's owner | `framework_ingestion/sources/<source_system>/` |
| `<source_system>_<table>.yaml` | The project bringing it in, or the source itself | `sources/<source_system>/<project>/`, or next to `source.yaml` when there is no project |
| `project.yaml` | The project | `framework_curation/<domain>/<project>/` |

Check them without a cluster: `./run.sh generate` for ingestion, `./run.sh projects:generate` for curation. Both print every error in one pass. Job runs, statuses, and definition metadata live in `framework_meta`; the views are listed in the [root README](../README.md#see-what-is-running).

## source.yaml

One per source system. The kind, raw volume, and foreign catalog are set here once, and every table in the source inherits them.

```yaml
source_system: billing           # must equal the folder name
domain: finance                  # must equal the parent folder name
owner: billing.owner@jll.com
kind: sftp_files                 # sftp_files, sharepoint_files, database, or cloud_files

raw_volume:                      # file kinds: where files land as delivered
  schema: billing_raw
  volume: landing

deliveries:
  daily:
    trigger: file_arrival        # fires when files land in the raw volume
  # or:  nightly: { schedule: "0 0 2 * * ?" }
```

| Field | Rule |
| --- | --- |
| `kind` | `sftp_files` and `sharepoint_files` need `raw_volume` (schema and volume). `database` needs `foreign_catalog` and takes an optional `engine` of `postgresql`, `azure_sql`, `sql_server`, or `db2`. `cloud_files` is the original raw-table path. |
| `deliveries` | At least one. Each has exactly one of `schedule` (a Quartz cron) or `trigger: file_arrival`. A database delivery has a schedule only. A `cloud_files` file-arrival delivery needs a `path` that starts with `abfss://` or `s3://`. |
| `fetch` | Rejected. Copying files from SFTP or SharePoint into the raw volume is not built. |

A delivery is the unit of deployment: every table in it loads in one Lakeflow pipeline and one job. A source with different cadences has several deliveries.

## Table spec: a file table

For `sftp_files` and `sharepoint_files`. The spec holds only what is true of the table.

```yaml
pipeline_id: billing_invoices     # starts with the source system name; unique
domain: finance
data_domain: invoices
team: billing_squad
technical_owner: billing.owner@jll.com
layer: bronze
job_name: billing_daily           # optional; must equal <source_system>_<delivery>
delivery: daily                   # optional when the source has one delivery
requested_by: billing             # the team that asked for this table

notify:
  on_failure: [billing.owner@jll.com]
  on_success: [billing.owner@jll.com]

data:
  classification: confidential    # internal, confidential, or highly_confidential
  pii: [pii_finance, pii_personal]

source:
  raw_path: invoices/             # the folder in the raw volume
  format: csv
  delimiter: ","
  header: true

raw_to_bronze:
  bronze: { schema: billing_bronze, table: invoices }
  dedup: true
  primary_keys: [invoice_id]
  watermark_column: updated_at
  columns:
    - name: invoice_id
      type: string
      description: Invoice identifier from the billing system
    - name: amount
      type: decimal(18,2)
      pii: [pii_finance]
  rules:
    - name: amount_non_negative
      expression: amount >= 0

quarantine: { schema: billing_quarantine, table: invoices }
```

`source` also takes `path_glob`, `max_files_per_trigger`, `header_rows` and `data_address` (Excel, both required), and `external_location`. Formats are `csv`, `delimited`, `text`, `json`, `parquet`, `avro`, `excel`. `fixed_width` and `yaml` are not supported from a raw volume yet. CSV and delimited files require `delimiter`.

## Table spec: a database table

For `database`. Every table of a delivery loads in the same run.

```yaml
pipeline_id: erp_customers
domain: finance
data_domain: customers
team: erp_squad
technical_owner: erp.owner@jll.com
layer: bronze
delivery: nightly
requested_by: erp

data: { classification: confidential, pii: [pii_basic] }

source:
  table: dbo.customers            # <schema>.<table> in the source database
  mode: merge                     # snapshot (default) or merge

raw_to_bronze:
  bronze: { schema: erp_bronze, table: customers }
  primary_keys: [customer_id]     # required for merge
  columns:
    - { name: customer_id, type: string }

quarantine: { schema: erp_quarantine, table: customers }
```

`snapshot` replaces bronze with the source table on every run. `merge` keeps bronze in step with the source on `primary_keys`. `dedup` is not used for database tables.

## Common spec rules

- `raw_to_bronze` holds the bronze table, keys, `columns`, and `rules`. `quarantine` names the table that receives rows that fail the rules.
- A column is `name`, `type`, and an optional `description`, which becomes the column comment. `classification` and `pii` on a column are only needed when they differ from the bronze table.
- `pii` values are `pii_health`, `pii_basic`, `pii_finance`, `pii_biometric`, `pii_personal`. Omit `pii` when there is none. A `raw`, `bronze`, or `quarantine` block can override `classification` or `pii`.
- `notify.on_failure` is who is mailed when the job fails. It defaults to the technical owner. The generated job's `email_notifications` carries the addresses of all its tables.
- Specs do not name a catalog. Every job passes the environment's catalog (`--catalog ${var.catalog}`), so one spec deploys to dev and prod. To validate by hand: `lakeflow-framework validate <folder> --catalog <catalog>`.
- Schemas default to `<source_system>_<layer>`. Do not put keys or passwords in a spec. Use a secret scope or a Unity Catalog connection.
- Older specs may say `project:` for the requesting team. It is read as `requested_by`. Specs under `sources/` must use `requested_by`.
- Two pipelines cannot write the same table, and `pipeline_id` is unique across the bundle.

## Table spec: cloud files (original shape)

`cloud_files` keeps the first design: files land in a raw table, then bronze.

```yaml
source_to_raw:
  method: autoloader              # autoloader, copy_into, or ctas
  source:
    system: billing
    format: csv
    path: abfss://landing@storage.dfs.core.windows.net/billing/invoices/
    delimiter: ","
  raw: { schema: billing_raw, table: invoices }
raw_to_bronze: { ... as above ... }
quarantine: { ... as above ... }
```

Excel cannot use `copy_into`. `fixed_width` needs `fixed_columns` (`name`, `start`, `length`) on the source. Paths must be `abfss://` or `s3://`.

## project.yaml

```yaml
project: billing_reporting        # must equal the folder name
domain: finance
owner: reporting.owner@jll.com

silver:
  customer_orders:
    triggered_by: [erp.customers, erp.orders]        # bronze tables
    deadline: { at: "07:00", on_miss: alert }
gold:
  revenue_daily:
    triggered_by: [silver.invoices_clean, silver.customer_orders]
    reads: [headcount_reporting.silver.headcount_clean]
    deadline: { at: "08:00", on_miss: alert }
products:
  revenue:
    triggered_by: [gold.revenue_daily]
    deadline: { at: "09:00", on_miss: alert }
```

| Reference | Meaning |
| --- | --- |
| `<source_system>.<table>` | A bronze table in the ingestion bundle |
| `silver.<name>`, `gold.<name>`, `product.<name>` | A table or data product of this project |
| `<project>.silver.<name>`, `<project>.product.<name>` | A table of another project |

A table depends only on lower layers: bronze, then silver, then gold, then a data product. `triggered_by` is required and fires the job once every listed table has refreshed. `reads` is read without waiting. `deadline.on_miss` is `alert` or `run_stale`. A table with more than one `triggered_by` and no deadline is flagged on the map. The same layer and name cannot be created by two projects, and projects cannot depend on each other in a loop.

## Tables the pipeline writes

For source system `billing` and table `invoices`:

- Files from a raw volume or a database: `<catalog>.billing_bronze.invoices` and `<catalog>.billing_quarantine.invoices`.
- Cloud files also write `<catalog>.billing_raw.invoices`.

Quarantine carries `failed_rules`, the names of the rules that failed. Rows that pass go to bronze. Volume-file rows also carry `_source_file` and `_ingested_at`, and database rows carry `_ingested_at`.

## Operations

The views are listed in the [root README](../README.md#see-what-is-running). The full run path is in [END_TO_END.md](END_TO_END.md).
