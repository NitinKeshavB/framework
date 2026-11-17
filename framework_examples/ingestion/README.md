# Ingestion examples

Specs to copy when adding a source system. They are not deployed. The bundle that runs is [framework_ingestion](../../framework_ingestion/README.md).

The first folder is the source system. A project is a child folder and must match `requested_by`. A spec with no project sits next to `source.yaml` and omits `requested_by`.

```
ingestion/sources/
  abs/                          sftp_files. Files stay in a raw volume.
    source.yaml
    abs_reference.yaml          no project
    fdh/abs_invoices.yaml       project fdh
  policies/                     sharepoint_files. Same raw-volume load, Excel files.
    source.yaml
    underwriting/policies_quotes.yaml
  claims_db/                    database. No raw layer. Snapshot and merge.
    source.yaml
    claims_db_codes.yaml        no project, snapshot
    actuarial/claims_db_claims.yaml
```

There is no `cloud_files` example: its methods (`autoloader`, `copy_into`, `ctas`) are not in the MVP, and validation rejects them.

`./run.sh generate` reads `framework_ingestion` only. To check these specs:

```bash
venv/bin/python -m lakeflow_framework.cli generate framework_examples/ingestion --check
```

`--check` changes nothing. Drop `--check` only if you want the generated pipelines and jobs written into this folder.
