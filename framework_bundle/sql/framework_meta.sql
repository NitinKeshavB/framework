-- Registry for Lakeflow source systems, schemas, workflows, pipelines, flows, securables,
-- and ownership refusals. ${catalog} is replaced by the DDL task.
-- registry_sync is the only writer of definitions. It runs at deploy, so these tables change
-- once per deploy, not once per job run.
--
-- asset_registry is SCD Type 2. Each version has valid_from, valid_to, and is_current. A
-- changed definition closes its current version and adds a new one. A definition removed
-- from the bundle closes its current version and adds a deletion version (is_deleted true)
-- that stays current until the definition comes back. Nothing in it is updated except
-- valid_to and is_current, and nothing is deleted.
-- asset_id is a stable SHA-256 of source coordinates (not the display full_name), so a rename
-- is a new version of the same asset rather than an unrelated object.
--
-- load_ledger is append-only recon (sole load-grain SoR). dataflow is the spec id
-- (joins v_pipelines). update_pipeline_uuid is the Lakeflow pipeline UUID from events.
-- ledger_detail holds file (bronze) or input (curated) child rows.
-- Hot run/recon tables keep about 90 days. Forever archives use PARTITIONED BY archive_quarter.
-- job_runs / job_task_runs are append-only. Views keep latest per key.
-- ownership_refusals replaces the vestigial run_ledger / run_current step tables.
--
-- Constraints are informational PRIMARY KEY (Unity Catalog) plus enforced CHECK on
-- enumerations, declared in CREATE TABLE for a clean first deploy. After a full
-- framework_meta wipe, deploy this file as-is (CREATE IF NOT EXISTS + seed INSERTs +
-- CREATE OR REPLACE VIEW). Do not keep DROP/ADD CONSTRAINT migration pairs here.

CREATE SCHEMA IF NOT EXISTS ${catalog}.framework_meta;

-- Per-delivery pipeline event logs. Kept out of framework_meta so SHOW TABLES there
-- does not grow with every job (hourly pipeline_updates and load metrics look here).
CREATE SCHEMA IF NOT EXISTS ${catalog}.framework_events;

-- Enumeration registration (discipline from the legacy Synapse framework).
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ref_object_kind (
  object_kind STRING NOT NULL,
  CONSTRAINT ref_object_kind_pk PRIMARY KEY (object_kind)
);
INSERT INTO ${catalog}.framework_meta.ref_object_kind (object_kind)
SELECT object_kind FROM (VALUES
  ('source_system'),
  ('schema'),
  ('workflow'),
  ('pipeline'),
  ('dataflow'),
  ('flow'),
  ('table'),
  ('streaming_table'),
  ('materialized_view'),
  ('column'),
  ('link')
) AS v(object_kind)
WHERE NOT EXISTS (SELECT 1 FROM ${catalog}.framework_meta.ref_object_kind r WHERE r.object_kind = v.object_kind);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ref_owner_kind (
  owner_kind STRING NOT NULL,
  CONSTRAINT ref_owner_kind_pk PRIMARY KEY (owner_kind)
);
INSERT INTO ${catalog}.framework_meta.ref_owner_kind (owner_kind)
SELECT owner_kind FROM (VALUES
  ('source_system'),
  ('project')
) AS v(owner_kind)
WHERE NOT EXISTS (SELECT 1 FROM ${catalog}.framework_meta.ref_owner_kind r WHERE r.owner_kind = v.owner_kind);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ref_relation (
  relation STRING NOT NULL,
  CONSTRAINT ref_relation_pk PRIMARY KEY (relation)
);
INSERT INTO ${catalog}.framework_meta.ref_relation (relation)
SELECT relation FROM (VALUES
  ('owns'),
  ('contains'),
  ('reads'),
  ('writes'),
  ('rejects_to'),
  ('column_of')
) AS v(relation)
WHERE NOT EXISTS (SELECT 1 FROM ${catalog}.framework_meta.ref_relation r WHERE r.relation = v.relation);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ref_layer (
  layer STRING NOT NULL,
  CONSTRAINT ref_layer_pk PRIMARY KEY (layer)
);
INSERT INTO ${catalog}.framework_meta.ref_layer (layer)
SELECT layer FROM (VALUES
  ('raw'),
  ('bronze'),
  ('quarantine'),
  ('silver'),
  ('gold'),
  ('product'),
  ('staged')
) AS v(layer)
WHERE NOT EXISTS (SELECT 1 FROM ${catalog}.framework_meta.ref_layer r WHERE r.layer = v.layer);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.asset_registry (
  asset_id STRING NOT NULL,
  object_kind STRING NOT NULL,
  full_name STRING NOT NULL,
  owner_kind STRING NOT NULL,
  owner STRING NOT NULL,
  requested_by STRING,
  delivery STRING,
  parent_full_name STRING,
  relation STRING,
  config_json STRING NOT NULL,
  content_hash STRING NOT NULL,
  source STRING NOT NULL,
  bundle_id STRING,
  recorded_at TIMESTAMP NOT NULL,
  config VARIANT,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP,
  is_current BOOLEAN NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  CONSTRAINT asset_registry_pk PRIMARY KEY (object_kind, full_name, valid_from),
  CONSTRAINT asset_registry_object_kind CHECK (object_kind IN ('source_system', 'schema', 'workflow', 'pipeline', 'dataflow', 'flow', 'table', 'streaming_table', 'materialized_view', 'column', 'link')),
  CONSTRAINT asset_registry_owner_kind CHECK (owner_kind IN ('source_system', 'project')),
  CONSTRAINT asset_registry_relation CHECK (relation IS NULL OR relation IN ('owns', 'contains', 'reads', 'writes', 'rejects_to', 'column_of'))
)
CLUSTER BY (owner, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.asset_registry_current (
  asset_id STRING NOT NULL,
  object_kind STRING NOT NULL,
  full_name STRING NOT NULL,
  owner_kind STRING NOT NULL,
  owner STRING NOT NULL,
  requested_by STRING,
  delivery STRING,
  parent_full_name STRING,
  relation STRING,
  config_json STRING NOT NULL,
  content_hash STRING NOT NULL,
  source STRING NOT NULL,
  bundle_id STRING,
  recorded_at TIMESTAMP NOT NULL,
  config VARIANT,
  CONSTRAINT asset_registry_current_pk PRIMARY KEY (object_kind, full_name),
  CONSTRAINT asset_registry_current_object_kind CHECK (object_kind IN ('source_system', 'schema', 'workflow', 'pipeline', 'dataflow', 'flow', 'table', 'streaming_table', 'materialized_view', 'column', 'link')),
  CONSTRAINT asset_registry_current_owner_kind CHECK (owner_kind IN ('source_system', 'project')),
  CONSTRAINT asset_registry_current_relation CHECK (relation IS NULL OR relation IN ('owns', 'contains', 'reads', 'writes', 'rejects_to', 'column_of'))
)
CLUSTER BY (owner, object_kind, full_name)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Ownership refusals only (replaces run_ledger / run_current step tables).
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ownership_refusals (
  object_kind STRING NOT NULL,
  full_name STRING NOT NULL,
  refused_owner_kind STRING NOT NULL,
  refused_owner STRING NOT NULL,
  holder_owner_kind STRING NOT NULL,
  holder_owner STRING NOT NULL,
  failure_reason STRING NOT NULL,
  content_hash STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT ownership_refusals_object_kind CHECK (object_kind IN ('source_system', 'schema', 'workflow', 'pipeline', 'dataflow', 'flow', 'table', 'streaming_table', 'materialized_view', 'column', 'link')),
  CONSTRAINT ownership_refusals_refused_owner_kind CHECK (refused_owner_kind IN ('source_system', 'project')),
  CONSTRAINT ownership_refusals_holder_owner_kind CHECK (holder_owner_kind IN ('source_system', 'project'))
)
CLUSTER BY (full_name, recorded_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.job_runs (
  job_name STRING NOT NULL,
  deployed_name STRING,
  workspace_id STRING,
  job_id STRING NOT NULL,
  run_id STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status STRING NOT NULL,
  life_cycle_state STRING,
  result_state STRING,
  termination_code STRING,
  failure_reason STRING,
  trigger_type STRING,
  run_as STRING,
  setup_seconds DOUBLE,
  queue_seconds DOUBLE,
  execution_seconds DOUBLE,
  run_seconds DOUBLE,
  run_page_url STRING,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT job_runs_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'SKIPPED', 'FAILED'))
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.job_task_runs (
  job_name STRING NOT NULL,
  workspace_id STRING,
  job_id STRING NOT NULL,
  job_run_id STRING NOT NULL,
  task_run_id STRING NOT NULL,
  task_key STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status STRING NOT NULL,
  life_cycle_state STRING,
  result_state STRING,
  termination_code STRING,
  failure_reason STRING,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT job_task_runs_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'SKIPPED', 'FAILED'))
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.pipeline_updates (
  job_name STRING NOT NULL,
  pipeline_id STRING NOT NULL,
  pipeline_name STRING,
  update_id STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  state STRING,
  status STRING NOT NULL,
  run_as STRING,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT pipeline_updates_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED'))
)
CLUSTER BY (job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Append-only recon. dataflow = spec id. update_pipeline_uuid = Lakeflow UUID from events.
-- Hot ~90 days (liquid CLUSTER BY). Forever archive PARTITIONED BY archive_quarter (Hive).
-- Child ledger_detail joins on (update_id, dataflow). batch_id bridges to row _batch_id only.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.load_ledger (
  layer STRING NOT NULL,
  job_name STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  update_id STRING NOT NULL,
  dataflow STRING NOT NULL,
  load_kind STRING NOT NULL,
  target_status STRING,
  load_date DATE NOT NULL,
  ended_at TIMESTAMP NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  batch_id STRING,
  job_run_id STRING,
  update_pipeline_uuid STRING,
  source_kind STRING,
  run_as STRING,
  target_table STRING,
  rows_read BIGINT,
  rows_inserted BIGINT,
  rows_updated BIGINT,
  rows_deleted BIGINT,
  rows_quarantined BIGINT,
  rows_dropped BIGINT,
  files_read BIGINT,
  files_failed BIGINT,
  failure_reason STRING,
  CONSTRAINT load_ledger_layer CHECK (layer IN ('raw', 'bronze', 'quarantine', 'silver', 'gold', 'product', 'staged')),
  CONSTRAINT load_ledger_load_kind CHECK (load_kind IN ('regular', 'backfill', 'catchup'))
)
CLUSTER BY (layer, job_name, started_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.load_ledger_archive (
  layer STRING NOT NULL,
  job_name STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  update_id STRING NOT NULL,
  dataflow STRING NOT NULL,
  load_kind STRING NOT NULL,
  target_status STRING,
  load_date DATE NOT NULL,
  ended_at TIMESTAMP NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  batch_id STRING,
  job_run_id STRING,
  update_pipeline_uuid STRING,
  source_kind STRING,
  run_as STRING,
  target_table STRING,
  rows_read BIGINT,
  rows_inserted BIGINT,
  rows_updated BIGINT,
  rows_deleted BIGINT,
  rows_quarantined BIGINT,
  rows_dropped BIGINT,
  files_read BIGINT,
  files_failed BIGINT,
  failure_reason STRING,
  archived_at TIMESTAMP NOT NULL,
  archive_quarter STRING NOT NULL
)
PARTITIONED BY (archive_quarter)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Scratch space for a backfill's Auto Loader state (schema and checkpoint).
CREATE VOLUME IF NOT EXISTS ${catalog}.framework_meta.backfill_state
COMMENT 'Checkpoints of framework_backfill runs. Safe to delete a run folder once it has finished.';

-- Child unit-of-work: detail_kind=file|input. Join parent on (update_id, dataflow). No batch_id.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ledger_detail (
  update_id STRING NOT NULL,
  dataflow STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  detail_kind STRING NOT NULL,
  status STRING NOT NULL,
  load_date DATE NOT NULL,
  job_name STRING NOT NULL,
  layer STRING NOT NULL,
  load_kind STRING,
  target_table STRING,
  src_object STRING,
  src_file_path STRING,
  src_file_name STRING,
  rows_read BIGINT,
  rows_inserted BIGINT,
  rows_quarantined BIGINT,
  rows_dropped BIGINT,
  failed_rules STRING,
  blocking_rules STRING,
  error_message STRING,
  CONSTRAINT ledger_detail_status CHECK (status IN ('PROCESSED', 'WARNING', 'FAILED')),
  CONSTRAINT ledger_detail_kind CHECK (detail_kind IN ('file', 'input'))
)
CLUSTER BY (update_id, dataflow, recorded_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ledger_detail_archive (
  update_id STRING NOT NULL,
  dataflow STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  detail_kind STRING NOT NULL,
  status STRING NOT NULL,
  load_date DATE NOT NULL,
  job_name STRING NOT NULL,
  layer STRING NOT NULL,
  load_kind STRING,
  target_table STRING,
  src_object STRING,
  src_file_path STRING,
  src_file_name STRING,
  rows_read BIGINT,
  rows_inserted BIGINT,
  rows_quarantined BIGINT,
  rows_dropped BIGINT,
  failed_rules STRING,
  blocking_rules STRING,
  error_message STRING,
  archived_at TIMESTAMP NOT NULL,
  archive_quarter STRING NOT NULL
)
PARTITIONED BY (archive_quarter)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.job_runs_archive (
  job_name STRING NOT NULL,
  deployed_name STRING,
  workspace_id STRING,
  job_id STRING NOT NULL,
  run_id STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status STRING NOT NULL,
  life_cycle_state STRING,
  result_state STRING,
  termination_code STRING,
  failure_reason STRING,
  trigger_type STRING,
  run_as STRING,
  setup_seconds DOUBLE,
  queue_seconds DOUBLE,
  execution_seconds DOUBLE,
  run_seconds DOUBLE,
  run_page_url STRING,
  recorded_at TIMESTAMP NOT NULL,
  archived_at TIMESTAMP NOT NULL,
  archive_quarter STRING NOT NULL
)
PARTITIONED BY (archive_quarter)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.job_task_runs_archive (
  job_name STRING NOT NULL,
  workspace_id STRING,
  job_id STRING NOT NULL,
  job_run_id STRING NOT NULL,
  task_run_id STRING NOT NULL,
  task_key STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status STRING NOT NULL,
  life_cycle_state STRING,
  result_state STRING,
  termination_code STRING,
  failure_reason STRING,
  recorded_at TIMESTAMP NOT NULL,
  archived_at TIMESTAMP NOT NULL,
  archive_quarter STRING NOT NULL
)
PARTITIONED BY (archive_quarter)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.pipeline_updates_archive (
  job_name STRING NOT NULL,
  pipeline_id STRING NOT NULL,
  pipeline_name STRING,
  update_id STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  state STRING,
  status STRING NOT NULL,
  run_as STRING,
  recorded_at TIMESTAMP NOT NULL,
  archived_at TIMESTAMP NOT NULL,
  archive_quarter STRING NOT NULL
)
PARTITIONED BY (archive_quarter)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.ddl_applied (
  sql_hash STRING NOT NULL,
  applied_at TIMESTAMP NOT NULL
);

-- Append-only flow checkpoint (incremental watermark for any flow: JDBC bronze, read_new, …).
-- Effective mark: v_flow_checkpoint (latest SUCCEEDED). Never MERGE/UPDATE the physical table.
CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.flow_checkpoint (
  reader_table STRING NOT NULL,
  source_table STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  load_kind STRING,
  progress_kind STRING,
  watermark_column STRING,
  watermark_ts TIMESTAMP,
  watermark_id STRING,
  job_name STRING,
  pipeline_id STRING,
  update_id STRING,
  batch_id STRING,
  CONSTRAINT flow_checkpoint_status CHECK (status IN ('SUCCEEDED', 'FAILED', 'STARTED'))
)
CLUSTER BY (reader_table, source_table)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_flow_checkpoint AS
SELECT
  reader_table,
  source_table,
  recorded_at,
  status,
  load_kind,
  progress_kind,
  watermark_column,
  watermark_ts,
  watermark_id,
  job_name,
  pipeline_id,
  update_id,
  batch_id
FROM ${catalog}.framework_meta.flow_checkpoint
WHERE status = 'SUCCEEDED'
QUALIFY row_number() OVER (
  PARTITION BY reader_table, source_table
  ORDER BY recorded_at DESC
) = 1;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_asset_registry_current AS
SELECT
  asset_id,
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
  bundle_id,
  recorded_at
FROM ${catalog}.framework_meta.asset_registry_current;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_ownership_refusals AS
SELECT
  object_kind,
  full_name,
  refused_owner_kind,
  refused_owner,
  holder_owner_kind,
  holder_owner,
  failure_reason,
  content_hash,
  recorded_at
FROM ${catalog}.framework_meta.ownership_refusals;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_lineage AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.asset_registry_current
WHERE object_kind = 'link';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_workflows AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'workflow';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_pipelines AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'pipeline';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_dataflows AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'dataflow';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_flows AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'flow';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_tables AS
SELECT
  asset_id,
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
  config:load_method::STRING AS load_method,
  config:load_type::STRING AS load_type,
  config:description::STRING AS description
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind IN ('table', 'streaming_table', 'materialized_view');

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_ownership_conflicts AS
WITH holders AS (
  SELECT DISTINCT
    object_kind,
    full_name,
    concat(owner_kind, ':', owner) AS holder
  FROM ${catalog}.framework_meta.asset_registry_current
  WHERE object_kind IN ('pipeline', 'table', 'streaming_table', 'materialized_view')
)
SELECT
  object_kind,
  full_name,
  sort_array(collect_list(holder)) AS owners
FROM holders
GROUP BY object_kind, full_name
HAVING count(*) > 1;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_asset_registry_history AS
SELECT
  asset_id,
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
  bundle_id,
  valid_from,
  valid_to,
  is_current,
  is_deleted
FROM ${catalog}.framework_meta.asset_registry;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_source_systems AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'source_system';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_schemas AS
SELECT
  asset_id,
  full_name AS schema_name,
  owner_kind,
  owner,
  config:layers::ARRAY<STRING> AS layers,
  config:tags AS tags,
  content_hash,
  recorded_at
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'schema';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_columns AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_current
WHERE object_kind = 'column';

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_column_history AS
SELECT
  asset_id,
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
FROM ${catalog}.framework_meta.v_asset_registry_history
WHERE object_kind = 'column';

-- Tags are parked (unity.APPLY_TAGS = False). Flip drift_enabled.on to true when tags apply.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_metadata_drift AS
WITH drift_enabled AS (
  SELECT false AS enabled
),
existing AS (
  SELECT lower(concat_ws('.', table_catalog, table_schema, table_name)) AS table_name
  FROM ${catalog}.information_schema.tables
),
existing_schemas AS (
  SELECT lower(concat_ws('.', catalog_name, schema_name)) AS schema_name
  FROM ${catalog}.information_schema.schemata
),
declared_schemas AS (
  SELECT lower(full_name) AS schema_name, owner_kind, owner, item, nullif(declared, '') AS declared
  FROM ${catalog}.framework_meta.asset_registry_current
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
  FROM ${catalog}.framework_meta.asset_registry_current
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
  FROM ${catalog}.framework_meta.asset_registry_current
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
JOIN drift_enabled ON drift_enabled.enabled
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
JOIN drift_enabled ON drift_enabled.enabled
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
JOIN drift_enabled ON drift_enabled.enabled
LEFT JOIN applied_columns a
  ON a.table_name = d.table_name AND a.column_name = d.column_name AND a.item = d.item
WHERE NOT (d.declared <=> a.applied)
  AND NOT (d.item = 'comment' AND d.declared IS NULL);

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
  run.failure_reason,
  run.trigger_type,
  run.run_as,
  run.setup_seconds,
  run.queue_seconds,
  run.execution_seconds,
  run.run_seconds,
  run.run_page_url,
  run.recorded_at
FROM ${catalog}.framework_meta.job_runs run
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = run.job_name
QUALIFY row_number() OVER (
  PARTITION BY run.job_id, run.run_id
  ORDER BY run.recorded_at DESC
) = 1;

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
  run.failure_reason,
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
  task.termination_code,
  task.failure_reason,
  task.recorded_at
FROM ${catalog}.framework_meta.job_task_runs task
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = task.job_name
QUALIFY row_number() OVER (
  PARTITION BY task.job_run_id, task.task_run_id
  ORDER BY task.recorded_at DESC
) = 1;

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
  pu.status,
  pu.run_as
FROM ${catalog}.framework_meta.pipeline_updates pu
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = pu.job_name;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_loads AS
WITH latest AS (
  SELECT *
  FROM ${catalog}.framework_meta.load_ledger
  QUALIFY row_number() OVER (
    PARTITION BY job_name, layer, update_id, dataflow
    ORDER BY recorded_at DESC
  ) = 1
)
SELECT
  l.layer,
  l.job_name,
  l.started_at,
  l.update_id,
  l.dataflow,
  l.load_kind,
  l.target_status,
  l.load_date,
  l.ended_at,
  l.recorded_at,
  l.batch_id,
  l.job_run_id,
  l.update_pipeline_uuid,
  l.source_kind,
  l.run_as,
  l.target_table,
  l.rows_read,
  l.rows_inserted,
  l.rows_updated,
  l.rows_deleted,
  l.rows_quarantined,
  coalesce(l.rows_dropped, 0) AS rows_dropped,
  coalesce(l.files_read, 0) AS files_read,
  coalesce(l.files_failed, 0) AS files_failed,
  l.failure_reason,
  (unix_millis(l.ended_at) - unix_millis(l.started_at)) / 1000.0 AS duration_seconds,
  p.owner_kind,
  p.owner,
  cast(d.quarantine AS STRING) AS quarantine_table,
  pu.pipeline_name
FROM latest l
LEFT JOIN ${catalog}.framework_meta.v_pipelines p ON p.pipeline = l.dataflow
LEFT JOIN ${catalog}.framework_meta.v_dataflows d ON d.dataflow = l.dataflow
LEFT JOIN ${catalog}.framework_meta.pipeline_updates pu
  ON pu.update_id = l.update_id
 AND (l.update_pipeline_uuid IS NULL OR pu.pipeline_id = l.update_pipeline_uuid);

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_ledger_detail AS
SELECT
  update_id,
  dataflow,
  recorded_at,
  detail_kind,
  status,
  load_date,
  job_name,
  layer,
  load_kind,
  target_table,
  src_object,
  src_file_path,
  src_file_name,
  rows_read,
  rows_inserted,
  rows_quarantined,
  rows_dropped,
  failed_rules,
  blocking_rules,
  error_message
FROM ${catalog}.framework_meta.ledger_detail
QUALIFY row_number() OVER (
  PARTITION BY job_name, layer, update_id, dataflow, detail_kind,
    coalesce(src_file_path, ''), coalesce(src_object, '')
  ORDER BY recorded_at DESC
) = 1;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_load_failures AS
SELECT
  l.job_name,
  l.layer,
  l.source_kind,
  l.run_as,
  l.load_kind,
  l.dataflow,
  l.target_table,
  l.update_id,
  l.update_pipeline_uuid,
  l.target_status,
  l.failure_reason AS load_failure_reason,
  l.rows_read,
  l.rows_inserted,
  l.rows_updated,
  l.rows_deleted,
  l.rows_quarantined,
  l.rows_dropped,
  l.files_read,
  l.files_failed,
  l.started_at AS load_started_at,
  l.ended_at AS load_ended_at,
  l.recorded_at AS load_recorded_at,
  l.load_date,
  l.batch_id,
  l.duration_seconds,
  l.owner_kind,
  l.owner,
  coalesce(l.job_run_id, j.run_id) AS job_run_id,
  j.status AS job_status,
  j.result_state AS job_result_state,
  j.termination_code,
  j.failure_reason AS job_failure_reason,
  j.run_as AS job_run_as,
  j.run_page_url,
  j.started_at AS job_started_at,
  t.task_key,
  t.status AS task_status,
  t.failure_reason AS task_failure_reason
FROM ${catalog}.framework_meta.v_loads l
LEFT JOIN ${catalog}.framework_meta.v_job_runs j
  ON j.job_name = l.job_name
 AND (
   (l.job_run_id IS NOT NULL AND j.run_id = l.job_run_id)
   OR (
     l.job_run_id IS NULL
     AND j.started_at <= coalesce(l.ended_at, l.recorded_at)
     AND (j.ended_at IS NULL OR j.ended_at >= coalesce(l.started_at, l.recorded_at))
   )
 )
LEFT JOIN ${catalog}.framework_meta.v_job_task_runs t
  ON t.job_run_id = j.run_id AND t.task_key = 'refresh_pipeline'
WHERE l.target_status IN ('FAILED', 'CANCELED')
   OR l.failure_reason IS NOT NULL
   OR j.status = 'FAILED'
QUALIFY row_number() OVER (
  PARTITION BY l.job_name, l.layer, l.update_id, l.dataflow
  ORDER BY j.started_at DESC NULLS LAST
) = 1;
