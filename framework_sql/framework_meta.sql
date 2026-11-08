-- Registry for Lakeflow workflows, pipelines, flows, securables, runs, and reconciliation.
-- ${catalog} is replaced by the DDL task. These statements do not delete rows.

CREATE SCHEMA IF NOT EXISTS ${catalog}.framework_meta;

CREATE VOLUME IF NOT EXISTS ${catalog}.framework_meta.framework_events;

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.object_registry (
  object_kind STRING,
  full_name STRING,
  project STRING,
  parent_full_name STRING,
  relation STRING,
  config_json STRING,
  content_hash STRING,
  source STRING,
  recorded_at TIMESTAMP
)
CLUSTER BY (project, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.run_ledger (
  run_id STRING,
  parent_run_id STRING,
  object_kind STRING,
  full_name STRING,
  project STRING,
  step_no INT,
  step_name STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  failure_reason STRING,
  recon_json STRING,
  recorded_at TIMESTAMP,
  event_date DATE
)
CLUSTER BY (project, event_date, full_name, run_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

CREATE BLOOMFILTER INDEX ON TABLE ${catalog}.framework_meta.object_registry
FOR COLUMNS (full_name OPTIONS (fpp=0.1, numItems=1000000));

CREATE BLOOMFILTER INDEX ON TABLE ${catalog}.framework_meta.run_ledger
FOR COLUMNS (
  run_id OPTIONS (fpp=0.1, numItems=10000000),
  full_name OPTIONS (fpp=0.1, numItems=1000000)
);

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_object_current AS
SELECT
  object_kind,
  full_name,
  project,
  parent_full_name,
  relation,
  config_json,
  content_hash,
  source,
  recorded_at
FROM ${catalog}.framework_meta.object_registry
QUALIFY row_number() OVER (
  PARTITION BY object_kind, full_name
  ORDER BY recorded_at DESC
) = 1;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_run_current AS
SELECT
  run_id,
  parent_run_id,
  object_kind,
  full_name,
  project,
  step_no,
  step_name,
  status,
  started_at,
  ended_at,
  failure_reason,
  recon_json,
  recorded_at,
  event_date
FROM ${catalog}.framework_meta.run_ledger
WHERE event_date >= date_sub(current_date(), 7)
QUALIFY row_number() OVER (
  PARTITION BY project, object_kind, full_name
  ORDER BY recorded_at DESC
) = 1;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_process AS
SELECT
  run_id,
  parent_run_id,
  object_kind,
  full_name,
  project,
  step_no,
  step_name,
  status,
  started_at,
  ended_at,
  failure_reason,
  recon_json,
  recorded_at,
  event_date
FROM ${catalog}.framework_meta.run_ledger;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_recon AS
SELECT
  run_id,
  project,
  full_name,
  event_date,
  recorded_at,
  recon.source_count AS source_count,
  recon.inserted AS inserted,
  recon.updated AS updated,
  recon.deleted AS deleted,
  recon.output_rows AS output_rows,
  recon.rejected AS rejected,
  recon.recon_result AS recon_result,
  recon.detail AS detail
FROM (
  SELECT
    run_id,
    project,
    full_name,
    event_date,
    recorded_at,
    from_json(
      recon_json,
      'source_count INT, inserted INT, updated INT, deleted INT, output_rows INT, rejected INT, recon_result STRING, detail STRING'
    ) AS recon
  FROM ${catalog}.framework_meta.run_ledger
  WHERE step_name = 'record_success'
) parsed;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_lineage AS
SELECT
  full_name,
  project,
  parent_full_name,
  relation,
  config_json,
  content_hash,
  source,
  recorded_at
FROM ${catalog}.framework_meta.object_registry
WHERE object_kind = 'link'
QUALIFY row_number() OVER (
  PARTITION BY parent_full_name, full_name, relation
  ORDER BY recorded_at DESC
) = 1;
