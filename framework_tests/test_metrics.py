"""Spark-free tests for ops wiring and the run / cost views."""

from __future__ import annotations

from pathlib import Path

import yaml

from lakeflow_framework.metadata import ddl_statements

BUNDLE = Path(__file__).resolve().parents[1] / "framework_bundle"


def test_run_views_need_no_system_tables_and_cost_views_need_only_billing():
    meta = ddl_statements("framework_dev")
    text = "\n".join(meta)
    for view in ("v_job_runs", "v_job_task_runs", "v_job_status", "v_pipeline_updates"):
        assert f"CREATE OR REPLACE VIEW framework_dev.framework_meta.{view}" in text
    for table in ("job_runs", "job_task_runs", "pipeline_updates"):
        assert f"CREATE TABLE IF NOT EXISTS framework_dev.framework_meta.{table}" in text
    assert "system." not in text.replace("system_views", "")
    system = "\n".join(
        ddl_statements("framework_dev", (BUNDLE / "sql" / "system_views.sql").read_text("utf-8"))
    )
    assert "system.lakeflow" not in system
    for view in ("v_job_costs", "v_pipeline_costs", "v_observed_lineage"):
        assert f"CREATE OR REPLACE VIEW framework_dev.framework_meta.{view}" in system
    for view in ("v_job_runs AS", "v_job_status AS", "v_pipeline_updates AS"):
        assert view not in system
    assert "usage_metadata.dlt_pipeline_id" in system and "usage_metadata.job_run_id" in system


def test_job_status_is_the_latest_recorded_run_per_workflow():
    statements = ddl_statements("framework_dev")
    runs_view = next(s for s in statements if "VIEW framework_dev.framework_meta.v_job_runs" in s)
    assert "PARTITION BY run.job_id, run.run_id" in runs_view
    assert "ORDER BY run.recorded_at DESC" in runs_view
    tasks_view = next(
        s for s in statements if "VIEW framework_dev.framework_meta.v_job_task_runs" in s
    )
    assert "PARTITION BY task.job_run_id, task.task_run_id" in tasks_view
    status = next(s for s in statements if "VIEW framework_dev.framework_meta.v_job_status" in s)
    assert "FROM framework_dev.framework_meta.v_job_runs" in status
    assert "PARTITION BY job_name ORDER BY started_at DESC" in status
    assert "LEFT JOIN latest run ON run.job_name = workflow.job_name" in status
    names = [s.split(" AS", 1)[0] for s in statements]
    runs = next(i for i, name in enumerate(names) if name.endswith("v_job_runs"))
    workflows = next(i for i, name in enumerate(names) if name.endswith("v_workflows"))
    assert workflows < runs < statements.index(status)


def test_ops_bundle_wires_ddl_system_views_runs_and_archive():
    resources = yaml.safe_load((BUNDLE / "resources" / "framework_ops.job.yml").read_text("utf-8"))
    jobs = resources["resources"]["jobs"]
    assert [task["task_key"] for task in jobs["framework_ddl"]["tasks"]] == ["apply_ddl"]
    assert jobs["framework_ddl"]["performance_target"] == "PERFORMANCE_OPTIMIZED"
    enabled, views = jobs["framework_system_views"]["tasks"]
    assert enabled["condition_task"] == {
        "op": "EQUAL_TO",
        "left": "${var.system_views}",
        "right": "true",
    }
    assert views["depends_on"] == [{"task_key": "system_views_enabled", "outcome": "true"}]
    assert views["spark_python_task"]["parameters"][1].endswith("sql/system_views.sql")
    bundle = yaml.safe_load((BUNDLE / "databricks.yml").read_text("utf-8"))
    assert bundle["variables"]["system_views"]["default"] == "false"
    # TEMPORARY: prod target uses the dev catalog until go-live; back to aal_prod_fdh then.
    assert bundle["targets"]["prod"]["variables"]["catalog"] == "aal_dev_fdh"
    runs = {task["task_key"]: task for task in jobs["framework_runs"]["tasks"]}
    assert set(runs) == {"job_runs", "pipeline_updates"}
    for part, task in runs.items():
        assert task["spark_python_task"]["parameters"] == ["${var.catalog}", part]
    assert "schedule" in jobs["framework_runs"]
    optimize = {task["task_key"] for task in jobs["framework_optimize"]["tasks"]}
    assert optimize == {"optimize", "archive_hot"}
    ddl = "\n".join(ddl_statements("framework_dev"))
    assert "v_loads" in ddl
    assert "v_load_failures" in ddl
    assert "ownership_refusals" in ddl
    assert "load_ledger_archive" in ddl
    assert "CREATE TABLE IF NOT EXISTS framework_dev.framework_meta.run_ledger" not in ddl
    assert "table_health" not in ddl
