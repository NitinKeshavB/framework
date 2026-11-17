-- Registry for Lakeflow source systems, schemas, workflows, pipelines, flows, securables,
-- ownership refusals, and table health. ${catalog} is replaced by the DDL task.
-- registry_sync is the only writer of definitions. It runs at deploy, so these tables change
-- once per deploy, not once per job run.
--
-- object_registry is SCD Type 2. Each version has valid_from, valid_to, and is_current. A
-- changed definition closes its current version and adds a new one. A definition removed
-- from the bundle closes its current version and adds a deletion version (is_deleted true)
-- that stays current until the definition comes back. Nothing in it is updated except
-- valid_to and is_current, and nothing is deleted. run_ledger and table_health are events
-- and stay insert-only. object_current and run_current hold one row per key
-- so a lookup does not window the history. job_runs, job_task_runs, and pipeline_updates are written hourly by
-- framework_runs from the Jobs API and the pipeline event logs, merged on their run or update
-- key, so a run that was still going is updated when it ends. They need no system table
-- grants. Per-load row counts are in bronze_load_ledger (appended by each delivery job). Cost is in system_views.sql.
--
-- Every row has an owner: owner_kind says what kind of owner (source_system for ingestion,
-- project for silver, gold, and data products) and owner is its name. A table has one owner. Bronze tables are
-- owned by their source system. requested_by is the team that asked for a bronze table, and
-- delivery is how its source delivers.
--
-- config is config_json parsed to VARIANT, so the views read it with config:path. config_json
-- stays: content_hash is computed over that text.
--
-- This file creates a fresh framework_meta. It has no ALTER statements, so a table created
-- with an older shape keeps it. Until go-live, drop the schema to change a table. After
-- go-live, add an ALTER here for each change instead.

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
  config VARIANT,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  is_deleted BOOLEAN
)
CLUSTER BY (owner, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Ownership refusals: a FAILED ownership_check step per refused pipeline or table.
-- Delivery jobs no longer write steps here. The step columns and recon_json stay so rows
-- from before that change still read.
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
  'delta.autoOptimize.autoCompact' = 'true'
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
  'delta.autoOptimize.autoCompact' = 'true'
);

-- One row per object: its latest run_ledger step. run_ledger keeps every step.
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
  'delta.autoOptimize.autoCompact' = 'true'
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

-- One row per run of a framework job, from the Jobs API. status is RUNNING, SUCCEEDED,
-- SKIPPED, or FAILED.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.job_runs (
  job_name STRING,
  deployed_name STRING,
  workspace_id STRING,
  job_id STRING,
  run_id STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status STRING,
  life_cycle_state STRING,
  result_state STRING,
  termination_code STRING,
  trigger_type STRING,
  setup_seconds DOUBLE,
  queue_seconds DOUBLE,
  execution_seconds DOUBLE,
  run_seconds DOUBLE,
  run_page_url STRING,
  recorded_at TIMESTAMP
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- One row per task run of a framework job run.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.job_task_runs (
  job_name STRING,
  workspace_id STRING,
  job_id STRING,
  job_run_id STRING,
  task_run_id STRING,
  task_key STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status STRING,
  life_cycle_state STRING,
  result_state STRING,
  termination_code STRING,
  recorded_at TIMESTAMP
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- One row per update of an ingestion pipeline, from the update_progress events in its event
-- log. state is the last update state. status is RUNNING, SUCCEEDED, FAILED, or CANCELED.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.pipeline_updates (
  job_name STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  update_id STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  state STRING,
  status STRING,
  recorded_at TIMESTAMP
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Append-only counts for each bronze table load. Each delivery job inserts a few rows after its
-- pipeline refresh, from that update's event log only. No MERGE. A retry may append again. The
-- view below keeps the latest recorded_at per job, update, and dataflow.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.bronze_load_ledger (
  job_name STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  update_id STRING,
  dataflow STRING,
  owner_kind STRING,
  owner STRING,
  bronze_table STRING,
  quarantine_table STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  rows_read BIGINT,
  rows_bronze BIGINT,
  rows_quarantine BIGINT,
  rows_processed BIGINT,
  bronze_status STRING,
  quarantine_status STRING,
  expectations STRING,
  recorded_at TIMESTAMP
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- One row per SQL file version apply_ddl has applied, by the hash of its statements. A deploy
-- whose SQL is already here skips the DDL. Delete the rows to make the next run apply it all.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ddl_applied (
  sql_hash STRING,
  applied_at TIMESTAMP
);

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

-- The latest step per object: an ownership refusal, or a job's last step from before run
-- outcomes moved to the system tables. v_job_status is the current run status.
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

-- Each definition registry_sync refused because another owner holds the name.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_ownership_refusals AS
SELECT
  object_kind,
  full_name,
  owner_kind AS refused_owner_kind,
  owner AS refused_owner,
  failure_reason,
  recorded_at,
  event_date
FROM ${catalog}.framework_meta.run_ledger
WHERE step_name = 'ownership_check' AND status = 'FAILED';

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
  config:source_system::STRING AS source_system,
  config:source_kind::STRING AS source_kind,
  config:delivery_detail.schedule::STRING AS schedule,
  config:delivery_detail.trigger::STRING AS delivery_trigger,
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
  config:pii::STRING AS pii,
  object_kind,
  delivery,
  config:layer::STRING AS layer,
  config:pipeline_id::STRING AS pipeline_id,
  config:job_name::STRING AS job_name,
  config:load_method::STRING AS load_method
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind IN ('table', 'streaming_table', 'materialized_view');

-- A table has one current owner. This view is empty when that holds. A row here means
-- object_current was written by hand with two rows for the same name.
-- The owners are made distinct first. Photon fails to compile a distinct count in HAVING
-- next to collect_set over the same expression.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_ownership_conflicts AS
WITH holders AS (
  SELECT DISTINCT
    object_kind,
    full_name,
    concat(owner_kind, ':', owner) AS holder
  FROM ${catalog}.framework_meta.object_current
  WHERE object_kind IN ('pipeline', 'table', 'streaming_table', 'materialized_view')
)
SELECT
  object_kind,
  full_name,
  sort_array(collect_list(holder)) AS owners
FROM holders
GROUP BY object_kind, full_name
HAVING count(*) > 1;

-- Every version of every object. A version was active during [valid_from, valid_to). A
-- null valid_to is still active. A deletion version (is_deleted) says the definition was
-- removed from its bundle at valid_from.
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
  is_current,
  is_deleted
FROM ${catalog}.framework_meta.object_registry;

-- Each source system as its source.yaml declares it.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_source_systems AS
SELECT
  full_name AS source_system,
  config:domain::STRING AS domain,
  config:owner_email::STRING AS owner_email,
  config:source_kind::STRING AS source_kind,
  config:engine::STRING AS engine,
  config:foreign_catalog::STRING AS foreign_catalog,
  config:raw_volume AS raw_volume,
  config:deliveries AS deliveries,
  config:requested_by::ARRAY<STRING> AS requesters,
  content_hash,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'source_system';

-- Each schema a source system reads or writes, with the tags registry_sync sets on it.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_schemas AS
SELECT
  full_name AS schema_name,
  owner_kind,
  owner,
  config:layers::ARRAY<STRING> AS layers,
  config:tags AS tags,
  content_hash,
  recorded_at
FROM ${catalog}.framework_meta.v_object_current
WHERE object_kind = 'schema';

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
-- Empty once registry_sync has run after the schemas and tables exist. Only schemas and
-- tables that exist are compared, and only the tags this framework sets. securable is
-- schema, table, or column. table_name is the schema's name on a schema row.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_metadata_drift AS
WITH existing AS (
  SELECT lower(concat_ws('.', table_catalog, table_schema, table_name)) AS table_name
  FROM ${catalog}.information_schema.tables
),
existing_schemas AS (
  SELECT lower(concat_ws('.', catalog_name, schema_name)) AS schema_name
  FROM ${catalog}.information_schema.schemata
),
declared_schemas AS (
  SELECT lower(full_name) AS schema_name, owner_kind, owner, item, nullif(declared, '') AS declared
  FROM ${catalog}.framework_meta.object_current
  LATERAL VIEW explode(map(
    'domain', config:tags.domain::STRING,
    'owner', config:tags.owner::STRING,
    'owner_email', config:tags.owner_email::STRING,
    'source_kind', config:tags.source_kind::STRING,
    'layer', config:tags.layer::STRING,
    'foreign_catalog', config:tags.foreign_catalog::STRING,
    'engine', config:tags.engine::STRING
  )) items AS item, declared
  WHERE object_kind = 'schema'
),
applied_schemas AS (
  SELECT
    lower(concat_ws('.', catalog_name, schema_name)) AS schema_name,
    tag_name AS item,
    tag_value AS applied
  FROM ${catalog}.information_schema.schema_tags
  WHERE tag_name IN (
    'domain', 'owner', 'owner_email', 'source_kind', 'layer', 'foreign_catalog', 'engine'
  )
),
declared_tables AS (
  SELECT lower(full_name) AS table_name, owner_kind, owner, item, nullif(declared, '') AS declared
  FROM ${catalog}.framework_meta.object_current
  LATERAL VIEW explode(map(
    'classification', config:classification::STRING,
    'domain', config:domain::STRING,
    'pii', config:pii::STRING,
    'data_domain', config:data_domain::STRING,
    'team', config:team::STRING,
    'technical_owner', config:technical_owner::STRING,
    'layer', config:layer::STRING,
    'pipeline_id', config:pipeline_id::STRING,
    'job_name', config:job_name::STRING,
    'load_method', config:load_method::STRING,
    'owner', owner,
    'requested_by', requested_by,
    'delivery', delivery
  )) items AS item, declared
  WHERE object_kind IN ('table', 'streaming_table', 'materialized_view')
),
applied_tables AS (
  SELECT
    lower(concat_ws('.', catalog_name, schema_name, table_name)) AS table_name,
    tag_name AS item,
    tag_value AS applied
  FROM ${catalog}.information_schema.table_tags
  WHERE tag_name IN (
    'classification', 'domain', 'pii', 'data_domain', 'team', 'technical_owner', 'layer',
    'pipeline_id', 'job_name', 'load_method', 'owner', 'requested_by', 'delivery'
  )
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
  'schema' AS securable,
  d.schema_name AS table_name,
  CAST(NULL AS STRING) AS column_name,
  d.owner_kind,
  d.owner,
  d.item,
  d.declared,
  a.applied
FROM declared_schemas d
JOIN existing_schemas e ON e.schema_name = d.schema_name
LEFT JOIN applied_schemas a ON a.schema_name = d.schema_name AND a.item = d.item
WHERE NOT (d.declared <=> a.applied)
UNION ALL
SELECT
  'table' AS securable,
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
  'column' AS securable,
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

-- Each run of a framework job, with its workflow's owner.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_job_runs AS
SELECT
  run.job_name,
  run.deployed_name,
  workflow.owner_kind,
  workflow.owner,
  workflow.requested_by,
  workflow.delivery,
  run.workspace_id,
  run.job_id,
  run.run_id,
  run.started_at,
  run.ended_at,
  run.status,
  run.life_cycle_state,
  run.result_state,
  run.termination_code,
  run.trigger_type,
  run.setup_seconds,
  run.queue_seconds,
  run.execution_seconds,
  run.run_seconds,
  run.run_page_url,
  run.recorded_at
FROM ${catalog}.framework_meta.job_runs run
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = run.job_name;

-- Each workflow and its latest run. A workflow that has not run has a null status.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_job_status AS
WITH latest AS (
  SELECT *
  FROM ${catalog}.framework_meta.v_job_runs
  QUALIFY row_number() OVER (PARTITION BY job_name ORDER BY started_at DESC) = 1
)
SELECT
  workflow.job_name,
  workflow.owner_kind,
  workflow.owner,
  workflow.requested_by,
  workflow.delivery,
  workflow.pipeline_ids,
  workflow.requesters,
  run.workspace_id,
  run.job_id,
  run.run_id,
  run.status,
  run.result_state,
  run.termination_code,
  run.started_at,
  run.ended_at,
  run.run_seconds,
  run.run_page_url
FROM ${catalog}.framework_meta.v_workflows workflow
LEFT JOIN latest run ON run.job_name = workflow.job_name;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_job_task_runs AS
SELECT
  task.job_name,
  workflow.owner_kind,
  workflow.owner,
  task.workspace_id,
  task.job_id,
  task.job_run_id,
  task.task_run_id,
  task.task_key,
  task.started_at,
  task.ended_at,
  task.status,
  task.result_state,
  task.termination_code
FROM ${catalog}.framework_meta.job_task_runs task
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = task.job_name;

-- Each update of a framework pipeline, from its event log.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_pipeline_updates AS
SELECT
  pu.job_name,
  pu.pipeline_name AS deployed_name,
  workflow.owner_kind,
  workflow.owner,
  pu.pipeline_id,
  pu.update_id,
  pu.started_at,
  pu.ended_at,
  (unix_millis(pu.ended_at) - unix_millis(pu.started_at)) / 1000.0 AS duration_seconds,
  pu.state,
  pu.status
FROM ${catalog}.framework_meta.pipeline_updates pu
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = pu.job_name;

-- Latest append for each load. Retries of record_load_metrics may insert again; this view
-- keeps one row per job, update, and dataflow.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_bronze_loads AS
SELECT
  job_name,
  pipeline_id,
  pipeline_name,
  update_id,
  dataflow,
  owner_kind,
  owner,
  bronze_table,
  quarantine_table,
  started_at,
  ended_at,
  duration_seconds,
  rows_read,
  rows_bronze,
  rows_quarantine,
  rows_processed,
  bronze_status,
  quarantine_status,
  expectations,
  recorded_at
FROM ${catalog}.framework_meta.bronze_load_ledger
QUALIFY row_number() OVER (
  PARTITION BY job_name, update_id, dataflow
  ORDER BY recorded_at DESC
) = 1;
