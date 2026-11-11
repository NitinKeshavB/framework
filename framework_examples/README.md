# Examples

[ingestion](ingestion/README.md) is the set of specs to copy. One source system per kind: `abs` (SFTP files in a raw volume, with and without a project), `policies` (SharePoint Excel), `claims_db` (a database, snapshot and merge), and `drops` (cloud files into a raw table, Auto Loader and COPY INTO).

`consumer` is the original single-bundle example: YAML under `configs/bronze`, a one-line pipeline file under `pipelines`, and a hand-written job. It uses the `cloud_files` shape, where files land in a raw table first.

It is kept as a reference and for its sample tests. Do not copy it for new work. New tables go in [framework_ingestion](../framework_ingestion/README.md), where pipelines and jobs are generated from the specs. Silver, gold, and data products go in [framework_curation](../framework_curation/README.md).

- It registers `billing_invoices`, which the ingestion sample also registers. Do not deploy both to the same catalog.
- Its specs still say `project:`. That is read as `requested_by`.
- Its jobs record steps with `--owner-kind project --owner finance`.
- Its job lists each config by hand and is not generated.
