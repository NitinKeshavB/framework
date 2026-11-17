"""Spark-free tests for table health, the framework_metrics pipeline, and the run views."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from lakeflow_framework.health import health_row, quoted, record_table_health
from lakeflow_framework.metadata import ddl_statements

BUNDLE = Path(__file__).resolve().parents[1] / "framework_bundle"
RECORDED = datetime(2026, 11, 17, tzinfo=timezone.utc)
TABLE = {
    "table_name": "framework_dev.billing_bronze.invoices",
    "owner_kind": "source_system",
    "owner": "billing",
}


def _metrics_module():
    spec = importlib.util.spec_from_file_location(
        "framework_metrics", BUNDLE / "src" / "framework_metrics.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Frame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0] if self.rows else None


class _HealthSpark:
    def __init__(self, history=None, fail_detail=False):
        self.history = history or []
        self.fail_detail = fail_detail
        self.statements: list[str] = []
        self.appended: list[tuple[str, list[dict]]] = []

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT table_name"):
            return _Frame([TABLE])
        if statement.startswith("DESCRIBE DETAIL"):
            if self.fail_detail:
                raise RuntimeError("not a Delta table")
            return _Frame([{"numFiles": 12, "sizeInBytes": 4096}])
        if statement.startswith("DESCRIBE HISTORY"):
            return _Frame(self.history)
        raise AssertionError(statement)

    def createDataFrame(self, rows, _schema):
        spark = self

        class Writer:
            def mode(self, _mode):
                return self

            def saveAsTable(self, name):
                spark.appended.append((name, rows))

        class Frame:
            write = Writer()

        return Frame()


def test_table_health_records_files_size_and_the_last_load_commit():
    history = [
        {"version": 9, "operation": "OPTIMIZE", "timestamp": RECORDED, "operationMetrics": {}},
        {
            "version": 8,
            "operation": "STREAMING UPDATE",
            "timestamp": RECORDED,
            "operationMetrics": {"numOutputRows": 40},
        },
    ]
    spark = _HealthSpark(history)
    assert record_table_health(spark, "framework_dev") == 1
    ((table, (row,)),) = spark.appended
    assert table == "framework_dev.framework_meta.table_health"
    assert (row["num_files"], row["size_in_bytes"]) == (12, 4096)
    assert (row["last_version"], row["last_operation"]) == (8, "STREAMING UPDATE")
    assert row["last_operation_metrics"] == {"numOutputRows": "40"}
    assert row["error"] is None and row["owner"] == "billing"
    assert "DESCRIBE DETAIL `framework_dev`.`billing_bronze`.`invoices`" in spark.statements


def test_a_table_that_cannot_be_described_is_recorded_with_its_error():
    row = health_row(_HealthSpark(fail_detail=True), TABLE, RECORDED)
    assert row["num_files"] is None
    assert "DESCRIBE DETAIL: not a Delta table" in row["error"]
    unsafe = health_row(_HealthSpark(), {**TABLE, "table_name": "a.b.c; DROP"}, RECORDED)
    assert "not a catalog.schema.table name" in unsafe["error"]
    with pytest.raises(ValueError):
        quoted("only.two")
    with pytest.raises(ValueError):
        record_table_health(_HealthSpark(), "bad catalog")


def test_metrics_pipeline_finds_each_event_log_and_names_its_job():
    metrics = _metrics_module()
    found = metrics.event_logs(
        ["abs_daily_event_log", "object_current", "erp_hourly_event_log", "bad-name_event_log"]
    )
    assert found == {"abs_daily_event_log": "abs_daily", "erp_hourly_event_log": "erp_hourly"}
    assert "'abs_daily' AS job_name" in metrics.event_columns("abs_daily")
    with pytest.raises(ValueError):
        metrics.event_columns("x' OR '1'='1")


def test_table_load_metrics_groups_by_pipeline_update_flow_and_table():
    sql = _metrics_module().table_load_metrics_sql("flow_events", "framework_dev.framework_meta")
    assert "GROUP BY job_name, pipeline_id, update_id, flow_name" in sql
    for field in (
        "num_output_rows",
        "num_upserted_rows",
        "num_deleted_rows",
        "backlog_bytes",
        "backlog_files",
        "dropped_records",
        "data_quality.expectations",
        "flow_definition.output_dataset",
    ):
        assert field in sql
    assert "duration_seconds" in sql
    assert "_staged" in sql  # quality counts are on the staging table, mapped to its dataflow
    assert "framework_dev.framework_meta.object_current" in sql


class _Pipelines:
    def __init__(self):
        self.tables: list[str] = []
        self.flows: list[tuple[str, str]] = []
        self.views: dict[str, object] = {}

    def create_streaming_table(self, name, **_options):
        self.tables.append(name)

    def temporary_view(self, name, **_options):
        def register(function):
            self.views[name] = function
            return function

        return register

    def append_flow(self, target, name):
        def register(function):
            self.flows.append((target, name))
            return function

        return register

    def materialized_view(self, name, **_options):
        def register(function):
            self.views[name] = function
            return function

        return register


class _MetricsSpark:
    def __init__(self, tables):
        self.tables = tables
        self.statements: list[str] = []

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SHOW TABLES"):
            return _Frame([type("Row", (), {"tableName": name}) for name in self.tables])
        return statement


def test_metrics_pipeline_adds_one_append_flow_per_event_log():
    metrics = _metrics_module()
    dp, spark = _Pipelines(), _MetricsSpark(["abs_daily_event_log", "erp_hourly_event_log"])
    metrics.register(dp, spark, "framework_dev")
    assert dp.tables == ["flow_events"]
    assert dp.flows == [
        ("flow_events", "abs_daily_events"),
        ("flow_events", "erp_hourly_events"),
    ]
    assert "abs_daily_event_log_src" in dp.views
    assert "erp_hourly_event_log_src" in dp.views
    assert "abs_daily_event_log" not in {name for _target, name in dp.flows}
    assert "FROM flow_events" in dp.views["table_load_metrics"]()


def test_metrics_pipeline_with_no_event_log_yet_keeps_an_empty_table():
    metrics = _metrics_module()
    dp, spark = _Pipelines(), _MetricsSpark(["object_current"])
    metrics.register(dp, spark, "framework_dev")
    assert dp.tables == [] and dp.flows == []
    assert "WHERE false) no_events" in dp.views["table_load_metrics"]()
    with pytest.raises(ValueError):
        metrics.register(dp, spark, "bad catalog")


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
    status = next(s for s in statements if "VIEW framework_dev.framework_meta.v_job_status" in s)
    assert "FROM framework_dev.framework_meta.v_job_runs" in status
    assert "PARTITION BY job_name ORDER BY started_at DESC" in status
    assert "LEFT JOIN latest run ON run.job_name = workflow.job_name" in status
    names = [s.split(" AS", 1)[0] for s in statements]
    runs = next(i for i, name in enumerate(names) if name.endswith("v_job_runs"))
    workflows = next(i for i, name in enumerate(names) if name.endswith("v_workflows"))
    assert workflows < runs < statements.index(status)


def test_ops_bundle_wires_metrics_system_views_and_table_health():
    resources = yaml.safe_load((BUNDLE / "resources" / "framework_ops.job.yml").read_text("utf-8"))
    jobs, pipelines = resources["resources"]["jobs"], resources["resources"]["pipelines"]
    # Missing system table grants must not stop the registry DDL, so the views are their own job.
    assert [task["task_key"] for task in jobs["framework_ddl"]["tasks"]] == ["apply_ddl"]
    # The system views are off unless the target sets system_views: the condition skips them.
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
    runs = {task["task_key"]: task for task in jobs["framework_runs"]["tasks"]}
    assert set(runs) == {"job_runs", "pipeline_updates"}
    for part, task in runs.items():
        assert task["spark_python_task"]["parameters"] == ["${var.catalog}", part]
    assert "schedule" in jobs["framework_runs"]
    health = {task["task_key"] for task in jobs["framework_optimize"]["tasks"]}
    assert health == {"optimize", "table_health"}
    pipeline = pipelines["framework_metrics_pipeline"]
    assert pipeline["serverless"] is True
    assert pipeline["configuration"] == {"lakeflow_framework.catalog": "${var.catalog}"}
    assert "event_log" not in pipeline  # its own log would be read back as an ingestion log
    refresh = jobs["framework_metrics"]["tasks"][0]["pipeline_task"]["pipeline_id"]
    assert refresh == "${resources.pipelines.framework_metrics_pipeline.id}"
    assert jobs["framework_metrics"]["performance_target"] == "PERFORMANCE_OPTIMIZED"
