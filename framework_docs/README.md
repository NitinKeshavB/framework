# Using the framework

The repository overview is [../README.md](../README.md). This page is the YAML contract.

Supply one YAML file per pipeline and a one-line pipeline file. The bundle builds the wheel.

## Install

Declare the wheel as a bundle artifact and point each job or pipeline library at `dist/*.whl`. `databricks bundle deploy` builds that wheel and uploads it. The example bundle in this repo already does this. There is no volume copy and no PyPI publish.

## Pipeline file

```python
from lakeflow_framework import build_pipeline

build_pipeline("billing_invoices")
```

`build_pipeline` loads the current row from `<catalog>.framework_meta.v_object_current`. The pipeline id must match the YAML `pipeline_id`.

## YAML

A file is a bronze load. `layer` is `bronze`. Two hops are required.

`source_to_raw` lands files in `<system>_raw`. `method` is `autoloader` (default), `copy_into`, or `ctas`. The source `path` is `abfss://` or `s3://`.

`raw_to_bronze` publishes the raw table to `<system>_bronze`. Quality rules and column types sit on this hop. `dedup: true` requires `primary_keys` and `watermark_column`.

`quarantine` names the `<system>_quarantine` table that receives rows that fail those rules.

```yaml
layer: bronze
source_to_raw:
  method: autoloader
  source:
    system: billing
    format: csv
    path: abfss://landing@storage.dfs.core.windows.net/billing/invoices/
    delimiter: ","
  raw:
    schema: billing_raw
    table: invoices
raw_to_bronze:
  bronze:
    schema: billing_bronze
    table: invoices
  dedup: true
  primary_keys: [invoice_id]
  watermark_column: updated_at
  rules:
    - name: amount_non_negative
      expression: amount >= 0
quarantine:
  schema: billing_quarantine
  table: invoices
```

Excel cannot use `copy_into`. Fixed-width requires `fixed_columns` with `name`, `start`, and `length` on the source. CSV and delimited files require `delimiter`. `catalog` is a real catalog name. `${var.catalog}` left in the file is an error, so render the catalog before `lakeflow-framework validate`.

Silver and gold use the same `<system>_<layer>` schema name later. This package accepts `layer: bronze` only.

Do not put account keys, passwords, or tokens in the file. Point `external_location` at a Unity Catalog external location.

Sample tests for these configs are in [framework_examples/consumer/tests](../framework_examples/consumer/tests). Copy that file beside your own bronze YAML. It checks the raw and bronze names and the load SQL. It does not start a cluster.

## Validate

```bash
lakeflow-framework validate configs/
```

The command lists every error and does not start a cluster. Run it in CI.

## Tables

For source system `billing` and table `invoices` the pipeline writes:

- `<catalog>.billing_raw.invoices`
- `<catalog>.billing_bronze.invoices`
- `<catalog>.billing_quarantine.invoices`

Quarantine includes `failed_rules`. Rows that pass the rules land in bronze. A later silver or gold schema can use the same `<source_system>_<layer>` name. This package does not build those layers.

## Operations

Ask these views, with a `project` filter:

- `v_object_current` for pipelines, workflows, and securables
- `v_run_current` for the latest step
- `v_process` for one `run_id`
- `v_recon` for source and target counts
- `v_lineage` for links written by the pipeline

The example job in [framework_examples/consumer](../framework_examples/consumer) writes the step files, waits until the collector has loaded the definitions, and then refreshes the billing and ledger pipelines. A success step can carry counts with `--recon-json '{"source_count": 10, "output_rows": 9, "rejected": 1}'`.

The full path, including the collector and the registry layout, is in [END_TO_END.md](END_TO_END.md).
