"""The ingestion examples under framework_examples load as real specs. No Spark."""

from pathlib import Path

from lakeflow_framework.ingestion import load_sources

EXAMPLES = Path(__file__).resolve().parents[1] / "framework_examples" / "ingestion"


def test_ingestion_examples_cover_each_kind() -> None:
    systems = {system.name: system for system in load_sources(EXAMPLES)}
    assert set(systems) == {"abs", "policies", "claims_db", "drops"}
    assert systems["abs"].kind == "sftp_files"
    assert systems["policies"].kind == "sharepoint_files"
    assert systems["claims_db"].kind == "database"
    assert systems["drops"].kind == "cloud_files"

    abs_tables = {table.config.pipeline_id: table for table in systems["abs"].tables}
    assert abs_tables["abs_reference"].requested_by == ""
    assert abs_tables["abs_invoices"].requested_by == "fdh"

    modes = {table.config.source_mode for table in systems["claims_db"].tables}
    assert modes == {"snapshot", "merge"}
    methods = {table.config.load_method for table in systems["drops"].tables}
    assert methods == {"autoloader", "copy_into"}
