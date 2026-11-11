"""Project structure: ownership, references, and the generated maps. No Spark."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from lakeflow_framework.cli import main
from lakeflow_framework.config import ConfigValidationError
from lakeflow_framework.projects import INDEX_NAME, MAP_NAME, generate, load_projects, new_project

REPO = Path(__file__).resolve().parents[1]
INGESTION = REPO / "framework_ingestion"
PROJECTS = REPO / "framework_projects"


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    shutil.copytree(PROJECTS, tmp_path / "projects", ignore=shutil.ignore_patterns("*.md"))
    return tmp_path / "projects"


def _project(root: Path, domain: str, name: str) -> Path:
    return root / domain / name / "project.yaml"


def _problems(root: Path) -> str:
    with pytest.raises(ConfigValidationError) as caught:
        load_projects(root, INGESTION)
    return " ".join(caught.value.errors)


def _edit(path: Path, find: str, replace: str) -> None:
    text = path.read_text("utf-8")
    assert find in text
    path.write_text(text.replace(find, replace), "utf-8")


def test_sample_projects_resolve_bronze_and_each_other(root: Path) -> None:
    projects, bronze = load_projects(root, INGESTION)
    assert {project.name for project in projects} == {"billing_reporting", "headcount_reporting"}
    assert ("erp", "orders") in bronze and ("billing", "invoices") in bronze


def test_generate_writes_maps_then_is_up_to_date(root: Path) -> None:
    written = generate(root, INGESTION)
    assert INDEX_NAME in written
    assert f"finance/billing_reporting/{MAP_NAME}" in written
    assert generate(root, INGESTION, check=True) == []
    assert main(["projects", "generate", str(root), "--sources", str(INGESTION), "--check"]) == 0


def test_map_shows_upstream_bronze_with_who_requested_it(root: Path) -> None:
    generate(root, INGESTION)
    text = (root / "finance/billing_reporting" / MAP_NAME).read_text("utf-8")
    assert "`erp.orders` (bronze, nightly delivery, requested by billing)" in text
    assert "`headcount_reporting.silver.headcount_clean` (project headcount_reporting)" in text
    other = (root / "people/headcount_reporting" / MAP_NAME).read_text("utf-8")
    assert "billing_reporting.gold.people_cost" not in other
    assert "`headcount_reporting.silver.headcount_clean` is used by billing_reporting" in other


def test_check_fails_when_a_table_is_added_without_regenerating(root: Path) -> None:
    generate(root, INGESTION)
    _edit(
        _project(root, "people", "headcount_reporting"),
        "gold:\n",
        "gold:\n  headcount_trend:\n    triggered_by: [silver.headcount_clean]\n",
    )
    assert generate(root, INGESTION, check=True)


def test_a_table_name_is_owned_by_one_project(root: Path) -> None:
    _edit(
        _project(root, "people", "headcount_reporting"),
        "gold:\n",
        "gold:\n  revenue_daily:\n    triggered_by: [silver.headcount_clean]\n",
    )
    assert "gold.revenue_daily is already owned by billing_reporting" in _problems(root)


def test_unknown_references_are_named(root: Path) -> None:
    path = _project(root, "finance", "billing_reporting")
    _edit(path, "[billing.invoices]", "[billing.nothing]")
    assert "billing.nothing is not a bronze table in the ingestion bundle" in _problems(root)


def test_own_and_other_project_references_are_checked(root: Path) -> None:
    path = _project(root, "finance", "billing_reporting")
    _edit(path, "[silver.invoices_clean]\n    reads", "[silver.missing]\n    reads")
    assert "silver.missing is not a table of this project" in _problems(root)


def test_a_project_cannot_name_itself_with_three_parts(root: Path) -> None:
    path = _project(root, "finance", "billing_reporting")
    _edit(path, "[silver.invoices_clean, silver.customer_orders]", "[billing_reporting.silver.invoices_clean]")
    assert "is your own table; write silver.invoices_clean" in _problems(root)


def test_a_table_can_only_depend_on_lower_layers(root: Path) -> None:
    path = _project(root, "finance", "billing_reporting")
    _edit(path, "[billing.invoices]", "[gold.revenue_daily]")
    assert "a silver table can only depend on lower layers" in _problems(root)


def test_projects_cannot_depend_on_each_other_in_a_loop(root: Path) -> None:
    _edit(
        _project(root, "people", "headcount_reporting"),
        "triggered_by: [hr_docs.headcount]",
        "triggered_by: [hr_docs.headcount]\n    reads: [billing_reporting.silver.invoices_clean]",
    )
    assert "depend on each other in a loop" in _problems(root)


def test_a_table_needs_a_trigger_and_a_valid_deadline(root: Path) -> None:
    path = _project(root, "finance", "billing_reporting")
    _edit(path, "[billing.invoices]", "[]")
    _edit(path, '{ at: "07:00", on_miss: alert }', '{ at: "7am", on_miss: ignore }')
    errors = _problems(root)
    assert "triggered_by needs at least one table" in errors
    assert "deadline.at must be a time such as 07:00" in errors


def test_fan_in_without_a_deadline_is_flagged_on_the_map(root: Path) -> None:
    _edit(
        _project(root, "finance", "billing_reporting"),
        '    deadline: { at: "07:00", on_miss: alert }\n',
        "",
    )
    generate(root, INGESTION)
    text = (root / "finance/billing_reporting" / MAP_NAME).read_text("utf-8")
    assert "silver.customer_orders waits for 2 tables and has no deadline" in text


def test_project_must_match_its_folder(root: Path) -> None:
    _edit(_project(root, "finance", "billing_reporting"), "project: billing_reporting", "project: other")
    assert "project must be billing_reporting, the folder name" in _problems(root)


def test_new_project_scaffolds_a_bundle_and_refuses_overwrite(tmp_path: Path) -> None:
    created = new_project(tmp_path, "retail", "sales_reporting", owner="sales.owner@jll.com")
    assert created == tmp_path / "retail/sales_reporting/project.yaml"
    assert (tmp_path / "retail/sales_reporting/databricks.yml").exists()
    with pytest.raises(ConfigValidationError):
        new_project(tmp_path, "retail", "sales_reporting", owner="sales.owner@jll.com")
    with pytest.raises(ConfigValidationError):
        new_project(tmp_path, "retail", "bad name", owner="not-an-email")


def test_an_empty_new_project_generates_a_map(root: Path) -> None:
    new_project(root, "retail", "sales_reporting", owner="sales.owner@jll.com")
    generate(root, INGESTION)
    assert "No silver tables yet." in (root / "retail/sales_reporting" / MAP_NAME).read_text("utf-8")


def test_invalid_ingestion_blocks_project_checks(root: Path, tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ConfigValidationError) as caught:
        load_projects(root, empty)
    assert "no sources folder" in " ".join(caught.value.errors)
