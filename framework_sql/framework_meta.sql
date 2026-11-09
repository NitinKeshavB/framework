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
CLUSTER BY (project, object_kind, full_name, recorded_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
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
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE ${catalog}.framework_meta.object_registry
CLUSTER BY (project, object_kind, full_name, recorded_at);

ALTER TABLE ${catalog}.framework_meta.object_registry
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE ${catalog}.framework_meta.run_ledger
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
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

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_pipelines AS
SELECT
  full_name AS pipeline,
  project,
  get_json_object(config_json, '$.config.domain') AS domain,
  get_json_object(config_json, '$.config.data_domain') AS data_domain,
  get_json_object(config_json, '$.config.team') AS team,
  get_json_object(config_json, '$.config.technical_owner') AS technical_owner,
  get_json_object(config_json, '$.config.notify.on_failure') AS failure_mail,
  get_json_object(config_json, '$.config.notify.on_success') AS success_mail
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'pipeline';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_tables AS
SELECT
  full_name AS table_name,
  project,
  get_json_object(config_json, '$.domain') AS domain,
  get_json_object(config_json, '$.data_domain') AS data_domain,
  get_json_object(config_json, '$.team') AS team,
  get_json_object(config_json, '$.technical_owner') AS technical_owner,
  get_json_object(config_json, '$.classification') AS classification,
  get_json_object(config_json, '$.pii') AS pii
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind IN ('table', 'streaming_table', 'materialized_view');

-- Every object kind keeps its versions here. valid_to is the next version's recorded_at.
-- A version was active during [valid_from, valid_to). A null valid_to is still active.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_object_history AS
SELECT
  object_kind,
  full_name,
  project,
  parent_full_name,
  relation,
  config_json,
  content_hash,
  source,
  valid_from,
  valid_to,
  valid_to IS NULL AS is_current
FROM (
  SELECT
    object_kind,
    full_name,
    project,
    parent_full_name,
    relation,
    config_json,
    content_hash,
    source,
    recorded_at AS valid_from,
    LEAD(recorded_at) OVER (
      PARTITION BY object_kind, full_name
      ORDER BY recorded_at, content_hash
    ) AS valid_to
  FROM ${catalog}.framework_meta.object_registry
) versions;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_columns AS
SELECT
  parent_full_name AS table_name,
  get_json_object(config_json, '$.name') AS column_name,
  project,
  CAST(get_json_object(config_json, '$.ordinal') AS INT) AS ordinal,
  get_json_object(config_json, '$.data_type') AS data_type,
  get_json_object(config_json, '$.description') AS description,
  get_json_object(config_json, '$.classification') AS classification,
  get_json_object(config_json, '$.pii') AS pii,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'column';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_column_history AS
SELECT
  parent_full_name AS table_name,
  get_json_object(config_json, '$.name') AS column_name,
  project,
  CAST(get_json_object(config_json, '$.ordinal') AS INT) AS ordinal,
  get_json_object(config_json, '$.data_type') AS data_type,
  get_json_object(config_json, '$.description') AS description,
  get_json_object(config_json, '$.classification') AS classification,
  get_json_object(config_json, '$.pii') AS pii,
  content_hash,
  valid_from,
  valid_to,
  is_current
FROM ${catalog}.framework_meta.v_object_history
WHERE object_kind = 'column';
