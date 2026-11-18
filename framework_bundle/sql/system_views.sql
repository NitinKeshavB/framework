-- Optional: cost and observed lineage from the system tables, joined to the framework's own
-- run tables. ${catalog} is replaced by the DDL task. framework_system_views runs this only when
-- the target sets system_views to true. The identity that runs it needs SELECT on
-- system.billing and system.access. Anyone who can read framework_meta can read these views,
-- cost included. Run status does not need this file: v_job_runs, v_job_status, and
-- v_pipeline_updates are in framework_meta.sql.
--
-- Cost is usage at list price, from system.billing.list_prices. A job run's cost includes
-- only its job compute. Its pipeline update is billed to the pipeline, in v_pipeline_costs.

-- Earlier versions created the run views here from system.lakeflow. framework_meta.sql now
-- creates them, and framework_ddl runs before this file, so nothing here replaces them.

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_job_costs AS
WITH runs AS (
  SELECT job_name, owner_kind, owner, workspace_id, job_id, run_id, started_at
  FROM ${catalog}.framework_meta.v_job_runs
),
usage AS (
  SELECT
    u.workspace_id,
    u.usage_metadata.job_id AS job_id,
    u.usage_metadata.job_run_id AS run_id,
    sum(u.usage_quantity) AS dbus,
    sum(u.usage_quantity * lp.pricing.default) AS list_cost,
    max(lp.currency_code) AS currency_code
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON lp.sku_name = u.sku_name
   AND lp.cloud = u.cloud
   AND lp.usage_unit = u.usage_unit
   AND u.usage_end_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  WHERE u.usage_metadata.job_id IN (SELECT job_id FROM runs)
  GROUP BY u.workspace_id, u.usage_metadata.job_id, u.usage_metadata.job_run_id
)
SELECT
  runs.job_name,
  runs.owner_kind,
  runs.owner,
  runs.workspace_id,
  runs.job_id,
  runs.run_id,
  runs.started_at,
  usage.dbus,
  usage.list_cost,
  usage.currency_code
FROM runs
JOIN usage
  ON usage.workspace_id = runs.workspace_id
 AND usage.job_id = runs.job_id
 AND usage.run_id = runs.run_id;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_pipeline_costs AS
WITH updates AS (
  SELECT job_name, owner_kind, owner, pipeline_id, update_id, started_at
  FROM ${catalog}.framework_meta.v_pipeline_updates
),
usage AS (
  SELECT
    u.workspace_id,
    u.usage_metadata.dlt_pipeline_id AS pipeline_id,
    u.usage_metadata.dlt_update_id AS update_id,
    sum(u.usage_quantity) AS dbus,
    sum(u.usage_quantity * lp.pricing.default) AS list_cost,
    max(lp.currency_code) AS currency_code
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON lp.sku_name = u.sku_name
   AND lp.cloud = u.cloud
   AND lp.usage_unit = u.usage_unit
   AND u.usage_end_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  WHERE u.usage_metadata.dlt_pipeline_id IN (SELECT pipeline_id FROM updates)
  GROUP BY
    u.workspace_id,
    u.usage_metadata.dlt_pipeline_id,
    u.usage_metadata.dlt_update_id
)
SELECT
  updates.job_name,
  updates.owner_kind,
  updates.owner,
  usage.workspace_id,
  updates.pipeline_id,
  updates.update_id,
  updates.started_at,
  usage.dbus,
  usage.list_cost,
  usage.currency_code
FROM updates
JOIN usage ON usage.pipeline_id = updates.pipeline_id AND usage.update_id = updates.update_id;

-- What Unity Catalog saw each bronze and quarantine table read, next to the source the spec
-- declares. The registry's link and flow rows stay the declared intent, and this is the
-- observed side. status is declared when the observed source is the declared raw table or under the
-- declared raw path, undeclared when it is something else, and not_observed when Unity Catalog
-- has no lineage for the table yet.
CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_observed_lineage AS
WITH dataflows AS (
  SELECT
    full_name AS dataflow,
    owner_kind,
    owner,
    lower(regexp_replace(config:raw::STRING, '/+$', '')) AS declared_source,
    lower(config:bronze::STRING) AS bronze,
    lower(config:quarantine::STRING) AS quarantine
  FROM ${catalog}.framework_meta.asset_registry_current
  WHERE object_kind = 'dataflow'
),
targets AS (
  SELECT dataflow, owner_kind, owner, declared_source, bronze AS target_table, 'bronze' AS layer
  FROM dataflows
  UNION ALL
  SELECT dataflow, owner_kind, owner, declared_source, quarantine, 'quarantine'
  FROM dataflows
),
observed AS (
  SELECT
    lower(target_table_full_name) AS target_table,
    lower(coalesce(source_table_full_name, source_path)) AS observed_source,
    max(source_type) AS source_type,
    max(event_time) AS last_seen,
    count(*) AS events
  FROM system.access.table_lineage
  WHERE lower(target_table_full_name) IN (SELECT target_table FROM targets)
    AND coalesce(source_table_full_name, source_path) IS NOT NULL
  GROUP BY lower(target_table_full_name), lower(coalesce(source_table_full_name, source_path))
)
SELECT
  t.dataflow,
  t.owner_kind,
  t.owner,
  t.layer,
  t.target_table,
  t.declared_source,
  o.observed_source,
  o.source_type,
  o.last_seen,
  o.events,
  CASE
    WHEN o.observed_source IS NULL THEN 'not_observed'
    WHEN o.observed_source = t.declared_source THEN 'declared'
    WHEN t.declared_source LIKE '/volumes/%'
      AND o.observed_source LIKE concat('%', t.declared_source, '%') THEN 'declared'
    ELSE 'undeclared'
  END AS status
FROM targets t
LEFT JOIN observed o ON o.target_table = t.target_table;
