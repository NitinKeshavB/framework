-- Registry for Lakeflow workflows, pipelines, flows, securables, runs, and table health.
-- ${catalog} is replaced by the DDL task. History tables are insert-only.
-- object_current and run_current hold one row per key so a lookup does not window the
-- history. registry_sync writes definitions at deploy and merges those rows. The backfill
-- below runs only while the current table is empty. Job run outcomes are not stored here.
-- v_job_runs and v_job_status in system_views.sql read them from the system tables.
--
-- Every row has an owner: owner_kind says what kind of owner (source_system for ingestion,
-- project for silver, gold, and data products) and owner is its name. A table has one owner. Bronze tables are
-- owned by their source system. requested_by is the team that asked for a bronze table, and
-- delivery is how its source delivers.
--
-- config is config_json parsed to VARIANT, so the views read it with config:path. config_json
-- stays: content_hash is computed over that text. Rows recorded before config existed are read
-- through parse_json(config_json).

CREATE SCHEMA IF NOT EXISTS ${catalog}.framework_meta;

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.object_registry (
  object_kind STRING,
  full_name STRING,
  owner_kind STRING,
  owner STRING,
  requested_by STRING,
  delivery STRING,
  parent_full_name STRING,
  relation STRING,
  config_json STRING,
  content_hash STRING,
  source STRING,
  recorded_at TIMESTAMP,
  config VARIANT
)
CLUSTER BY (owner, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'false'
);

-- recon_json is no longer written. It stays so older rows still read.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.run_ledger (
  run_id STRING,
  parent_run_id STRING,
  object_kind STRING,
  full_name STRING,
  owner_kind STRING,
  owner STRING,
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
CLUSTER BY (owner, event_date, run_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'false'
);

-- One row per object. Lookups and dashboards read this, not the history window.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.object_current (
  object_kind STRING,
  full_name STRING,
  owner_kind STRING,
  owner STRING,
  requested_by STRING,
  delivery STRING,
  parent_full_name STRING,
  relation STRING,
  config_json STRING,
  content_hash STRING,
  source STRING,
  recorded_at TIMESTAMP,
  config VARIANT
)
CLUSTER BY (owner, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'false'
);

-- One row per job or pipeline: the latest step. run_ledger keeps every step.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.run_current (
  run_id STRING,
  parent_run_id STRING,
  object_kind STRING,
  full_name STRING,
  owner_kind STRING,
  owner STRING,
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
CLUSTER BY (owner, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'false'
);

-- One row per bronze table per framework_optimize run.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.table_health (
  table_name STRING,
  owner_kind STRING,
  owner STRING,
  num_files BIGINT,
  size_in_bytes BIGINT,
  last_version BIGINT,
  last_operation STRING,
  last_commit_at TIMESTAMP,
  last_operation_metrics MAP<STRING, STRING>,
  error STRING,
  recorded_at TIMESTAMP,
  event_date DATE
)
CLUSTER BY (owner, event_date, table_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Tables created before config existed get the column here. The DDL task skips this
-- statement when the column is already there.
ALTER TABLE ${catalog}.framework_meta.object_registry ADD COLUMNS (config VARIANT);

ALTER TABLE ${catalog}.framework_meta.object_current ADD COLUMNS (config VARIANT);

-- Liquid clustering keys can change in place. New writes and OPTIMIZE use the new keys.
ALTER TABLE ${catalog}.framework_meta.object_registry CLUSTER BY (owner, object_kind, full_name);

ALTER TABLE ${catalog}.framework_meta.run_ledger CLUSTER BY (owner, event_date, run_id);

-- object_current is not history, so its older rows are parsed once here.
UPDATE ${catalog}.framework_meta.object_current
SET config = parse_json(config_json)
WHERE config IS NULL AND config_json IS NOT NULL;

INSERT INTO ${catalog}.framework_meta.object_current (
  object_kind,
  full_name,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name,
  relation,
  config_json,
  content_hash,
  source,
  recorded_at,
  config
)
SELECT
  object_kind,
  full_name,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name,
  relation,
  config_json,
  content_hash,
  source,
  recorded_at,
  coalesce(config, parse_json(config_json)) AS config
FROM ${catalog}.framework_meta.object_registry
WHERE (SELECT COUNT(*) FROM ${catalog}.framework_meta.object_current) = 0
QUALIFY row_number() OVER (
  PARTITION BY object_kind, full_name
  ORDER BY recorded_at DESC
) = 1;

INSERT INTO ${catalog}.framework_meta.run_current
SELECT
  run_id,
  parent_run_id,
  object_kind,
  full_name,
  owner_kind,
  owner,
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
WHERE (SELECT COUNT(*) FROM ${catalog}.framework_meta.run_current) = 0
QUALIFY row_number() OVER (
  PARTITION BY owner_kind, owner, object_kind, full_name
  ORDER BY recorded_at DESC
) = 1;

-- Load counts come from framework_meta.table_load_metrics, which framework_metrics builds.
DROP VIEW IF EXISTS ${catalog}.framework_meta.v_recon;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_object_current AS
SELECT
  object_kind,
  full_name,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name,
  relation,
  config_json,
  config,
  content_hash,
  source,
  recorded_at
FROM ${catalog}.framework_meta.object_current;

-- Every job's latest step, however long ago it ran: a weekly or monthly job still shows.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_run_current AS
SELECT
  run_id,
  parent_run_id,
  object_kind,
  full_name,
  owner_kind,
  owner,
  step_no,
  step_name,
  status,
  started_at,
  ended_at,
  failure_reason,
  recorded_at,
  event_date
FROM ${catalog}.framework_meta.run_current;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_process AS
SELECT
  run_id,
  parent_run_id,
  object_kind,
  full_name,
  owner_kind,
  owner,
  step_no,
  step_name,
  status,
  started_at,
  ended_at,
  failure_reason,
  recorded_at,
  event_date
FROM ${catalog}.framework_meta.run_ledger;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_lineage AS
SELECT
  full_name,
  owner_kind,
  owner,
  parent_full_name,
  relation,
  config_json,
  config,
  content_hash,
  source,
  recorded_at
FROM ${catalog}.framework_meta.object_current
WHERE object_kind = 'link';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_workflows AS
SELECT
  full_name AS job_name,
  owner_kind,
  owner,
  requested_by,
  delivery,
  config:pipeline_ids::ARRAY<STRING> AS pipeline_ids,
  config:requested_by::ARRAY<STRING> AS requesters,
  content_hash,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'workflow';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_pipelines AS
SELECT
  full_name AS pipeline,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name AS job_name,
  config:config.domain::STRING AS domain,
  config:config.data_domain::STRING AS data_domain,
  config:config.team::STRING AS team,
  config:config.technical_owner::STRING AS technical_owner,
  config:config.notify.on_failure AS failure_mail,
  config:config.notify.on_success AS success_mail
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'pipeline';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_dataflows AS
SELECT
  full_name AS dataflow,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name AS pipeline,
  config:raw AS raw,
  config:bronze AS bronze,
  config:quarantine AS quarantine,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'dataflow';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_flows AS
SELECT
  full_name AS flow,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name AS dataflow,
  relation,
  config:layer::STRING AS layer,
  config:path::STRING AS path,
  config:table::STRING AS table_name,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'flow';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_tables AS
SELECT
  full_name AS table_name,
  owner_kind,
  owner,
  requested_by,
  config:domain::STRING AS domain,
  config:data_domain::STRING AS data_domain,
  config:team::STRING AS team,
  config:technical_owner::STRING AS technical_owner,
  config:classification::STRING AS classification,
  config:pii::STRING AS pii
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind IN ('table', 'streaming_table', 'materialized_view');

-- A table has one current owner. This view is empty when that holds. A row here means
-- object_current was written by hand with two rows for the same name.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_ownership_conflicts AS
SELECT
  object_kind,
  full_name,
  collect_set(concat(owner_kind, ':', owner)) AS owners
FROM ${catalog}.framework_meta.object_current
WHERE object_kind IN ('pipeline', 'table', 'streaming_table', 'materialized_view')
GROUP BY object_kind, full_name
HAVING count(DISTINCT concat(owner_kind, ':', owner)) > 1;

-- Every object kind keeps its versions here. valid_to is the next version's recorded_at.
-- A version was active during [valid_from, valid_to). A null valid_to is still active.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_object_history AS
SELECT
  object_kind,
  full_name,
  owner_kind,
  owner,
  requested_by,
  delivery,
  parent_full_name,
  relation,
  config_json,
  config,
  content_hash,
  source,
  valid_from,
  valid_to,
  valid_to IS NULL AS is_current
FROM (
  SELECT
    object_kind,
    full_name,
    owner_kind,
    owner,
    requested_by,
    delivery,
    parent_full_name,
    relation,
    config_json,
    coalesce(config, parse_json(config_json)) AS config,
    content_hash,
    source,
    recorded_at AS valid_from,
    LEAD(recorded_at) OVER (
      PARTITION BY owner_kind, owner, object_kind, full_name
      ORDER BY recorded_at, content_hash
    ) AS valid_to
  FROM ${catalog}.framework_meta.object_registry
) versions;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_columns AS
SELECT
  parent_full_name AS table_name,
  config:name::STRING AS column_name,
  owner_kind,
  owner,
  config:ordinal::INT AS ordinal,
  config:data_type::STRING AS data_type,
  config:description::STRING AS description,
  config:classification::STRING AS classification,
  config:pii::STRING AS pii,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'column';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_column_history AS
SELECT
  parent_full_name AS table_name,
  config:name::STRING AS column_name,
  owner_kind,
  owner,
  config:ordinal::INT AS ordinal,
  config:data_type::STRING AS data_type,
  config:description::STRING AS description,
  config:classification::STRING AS classification,
  config:pii::STRING AS pii,
  content_hash,
  valid_from,
  valid_to,
  is_current
FROM ${catalog}.framework_meta.v_object_history
WHERE object_kind = 'column';

-- Bronze tables, for the daily table health step.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_bronze_tables AS
SELECT DISTINCT
  config:table::STRING AS table_name,
  owner_kind,
  owner
FROM ${catalog}.framework_meta.object_current
WHERE object_kind = 'flow'
  AND config:layer::STRING = 'bronze';

-- Declared tags and column comments that Unity Catalog does not hold, or holds differently.
-- Empty when every apply_metadata task has run since the last spec change. Only tables that
-- exist are compared, and only the tags this framework sets.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_metadata_drift AS
WITH existing AS (
  SELECT lower(concat_ws('.', table_catalog, table_schema, table_name)) AS table_name
  FROM ${catalog}.information_schema.tables
),
declared_tables AS (
  SELECT lower(full_name) AS table_name, owner_kind, owner, item, nullif(declared, '') AS declared
  FROM ${catalog}.framework_meta.object_current
  LATERAL VIEW explode(map(
    'classification', config:classification::STRING,
    'domain', config:domain::STRING,
    'owner', owner,
    'pii', config:pii::STRING
  )) items AS item, declared
  WHERE object_kind IN ('table', 'streaming_table', 'materialized_view')
),
applied_tables AS (
  SELECT
    lower(concat_ws('.', catalog_name, schema_name, table_name)) AS table_name,
    tag_name AS item,
    tag_value AS applied
  FROM ${catalog}.information_schema.table_tags
  WHERE tag_name IN ('classification', 'domain', 'owner', 'pii')
),
declared_columns AS (
  SELECT
    lower(parent_full_name) AS table_name,
    lower(config:name::STRING) AS column_name,
    owner_kind,
    owner,
    item,
    nullif(declared, '') AS declared
  FROM ${catalog}.framework_meta.object_current
  LATERAL VIEW explode(map(
    'classification', config:classification::STRING,
    'pii', config:pii::STRING,
    'comment', config:description::STRING
  )) items AS item, declared
  WHERE object_kind = 'column'
),
applied_columns AS (
  SELECT
    lower(concat_ws('.', catalog_name, schema_name, table_name)) AS table_name,
    lower(column_name) AS column_name,
    tag_name AS item,
    tag_value AS applied
  FROM ${catalog}.information_schema.column_tags
  WHERE tag_name IN ('classification', 'pii')
  UNION ALL
  SELECT
    lower(concat_ws('.', table_catalog, table_schema, table_name)) AS table_name,
    lower(column_name) AS column_name,
    'comment' AS item,
    comment AS applied
  FROM ${catalog}.information_schema.columns
  WHERE comment IS NOT NULL
)
SELECT
  d.table_name,
  CAST(NULL AS STRING) AS column_name,
  d.owner_kind,
  d.owner,
  d.item,
  d.declared,
  a.applied
FROM declared_tables d
JOIN existing e ON e.table_name = d.table_name
LEFT JOIN applied_tables a ON a.table_name = d.table_name AND a.item = d.item
WHERE NOT (d.declared <=> a.applied)
UNION ALL
SELECT
  d.table_name,
  d.column_name,
  d.owner_kind,
  d.owner,
  d.item,
  d.declared,
  a.applied
FROM declared_columns d
JOIN existing e ON e.table_name = d.table_name
LEFT JOIN applied_columns a
  ON a.table_name = d.table_name AND a.column_name = d.column_name AND a.item = d.item
WHERE NOT (d.declared <=> a.applied)
  AND NOT (d.item = 'comment' AND d.declared IS NULL);
