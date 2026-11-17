-- Job, task, and pipeline runs from the system tables, joined to the registry.
-- ${catalog} is replaced by the DDL task. The identity that runs it needs SELECT on
-- system.lakeflow and system.billing. Anyone who can read framework_meta can read these views,
-- cost included.
--
-- Deployed names carry a dev or prod prefix, so jobs are matched on the
-- lakeflow_framework_workflow tag the generator sets. A pipeline is named after its job, so a
-- pipeline is matched on that name with or without the "[...] " prefix.
-- The system tables cover the whole account: workspace_id says which workspace ran it.
-- Cost is usage at list price, from system.billing.list_prices.

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_job_runs AS
WITH jobs AS (
  SELECT
    workspace_id,
    job_id,
    name AS deployed_name,
    tags['lakeflow_framework_workflow'] AS job_name
  FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
),
framework_jobs AS (
  SELECT jobs.*
  FROM jobs
  JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = jobs.job_name
),
-- A long run is split into hourly rows. The durations are set on the last row only.
runs AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    min(period_start_time) AS started_at,
    max(period_end_time) AS ended_at,
    max_by(result_state, period_end_time) AS result_state,
    max_by(termination_code, period_end_time) AS termination_code,
    max(trigger_type) AS trigger_type,
    sum(setup_duration_seconds) AS setup_seconds,
    sum(queue_duration_seconds) AS queue_seconds,
    sum(execution_duration_seconds) AS execution_seconds,
    sum(run_duration_seconds) AS run_seconds
  FROM system.lakeflow.job_run_timeline
  WHERE job_id IN (SELECT job_id FROM framework_jobs)
  GROUP BY workspace_id, job_id, run_id
),
costs AS (
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
  WHERE u.usage_metadata.job_id IN (SELECT job_id FROM framework_jobs)
  GROUP BY u.workspace_id, u.usage_metadata.job_id, u.usage_metadata.job_run_id
)
SELECT
  job.job_name,
  job.deployed_name,
  workflow.owner_kind,
  workflow.owner,
  workflow.requested_by,
  workflow.delivery,
  run.workspace_id,
  run.job_id,
  run.run_id,
  run.started_at,
  run.ended_at,
  run.result_state,
  run.termination_code,
  run.trigger_type,
  run.setup_seconds,
  run.queue_seconds,
  run.execution_seconds,
  run.run_seconds,
  cost.dbus,
  cost.list_cost,
  cost.currency_code
FROM runs run
JOIN framework_jobs job ON job.workspace_id = run.workspace_id AND job.job_id = run.job_id
JOIN ${catalog}.framework_meta.v_workflows workflow ON workflow.job_name = job.job_name
LEFT JOIN costs cost
  ON cost.workspace_id = run.workspace_id
 AND cost.job_id = run.job_id
 AND cost.run_id = run.run_id;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_job_task_runs AS
WITH runs AS (
  SELECT DISTINCT workspace_id, job_id, job_name, owner_kind, owner
  FROM ${catalog}.framework_meta.v_job_runs
)
SELECT
  runs.job_name,
  runs.owner_kind,
  runs.owner,
  task.workspace_id,
  task.job_id,
  task.job_run_id,
  task.run_id AS task_run_id,
  task.task_key,
  min(task.period_start_time) AS started_at,
  max(task.period_end_time) AS ended_at,
  max_by(task.result_state, task.period_end_time) AS result_state,
  max_by(task.termination_code, task.period_end_time) AS termination_code
FROM system.lakeflow.job_task_run_timeline task
JOIN runs ON runs.workspace_id = task.workspace_id AND runs.job_id = task.job_id
GROUP BY
  runs.job_name,
  runs.owner_kind,
  runs.owner,
  task.workspace_id,
  task.job_id,
  task.job_run_id,
  task.run_id,
  task.task_key;

CREATE OR REPLACE VIEW ${catalog}.framework_meta.v_pipeline_updates AS
WITH pipelines AS (
  SELECT workspace_id, pipeline_id, name AS deployed_name
  FROM system.lakeflow.pipelines
  QUALIFY row_number() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
),
framework_pipelines AS (
  SELECT pipelines.*, workflow.job_name, workflow.owner_kind, workflow.owner
  FROM pipelines
  JOIN ${catalog}.framework_meta.v_workflows workflow
    ON pipelines.deployed_name = workflow.job_name
    OR endswith(pipelines.deployed_name, concat('] ', workflow.job_name))
),
updates AS (
  SELECT
    workspace_id,
    pipeline_id,
    update_id,
    max(update_type) AS update_type,
    max(trigger_type) AS trigger_type,
    min(period_start_time) AS started_at,
    max(period_end_time) AS ended_at,
    max_by(result_state, period_end_time) AS result_state
  FROM system.lakeflow.pipeline_update_timeline
  WHERE pipeline_id IN (SELECT pipeline_id FROM framework_pipelines)
  GROUP BY workspace_id, pipeline_id, update_id
),
costs AS (
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
  WHERE u.usage_metadata.dlt_pipeline_id IN (SELECT pipeline_id FROM framework_pipelines)
  GROUP BY
    u.workspace_id,
    u.usage_metadata.dlt_pipeline_id,
    u.usage_metadata.dlt_update_id
)
SELECT
  pipeline.job_name,
  pipeline.deployed_name,
  pipeline.owner_kind,
  pipeline.owner,
  pu.workspace_id,
  pu.pipeline_id,
  pu.update_id,
  pu.update_type,
  pu.trigger_type,
  pu.started_at,
  pu.ended_at,
  (unix_millis(pu.ended_at) - unix_millis(pu.started_at)) / 1000.0 AS duration_seconds,
  pu.result_state,
  cost.dbus,
  cost.list_cost,
  cost.currency_code
FROM updates pu
JOIN framework_pipelines pipeline
  ON pipeline.workspace_id = pu.workspace_id AND pipeline.pipeline_id = pu.pipeline_id
LEFT JOIN costs cost
  ON cost.workspace_id = pu.workspace_id
 AND cost.pipeline_id = pu.pipeline_id
 AND cost.update_id = pu.update_id;
