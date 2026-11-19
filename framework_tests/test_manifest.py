"""The delivery manifest is built before the target is known; the pipeline fills in the catalog."""

import json

from lakeflow_framework.pipeline import _configs_from_manifest, _with_catalog
from lakeflow_framework.sources import PLACEHOLDER_CATALOG

DOCUMENT = {
    "catalog": PLACEHOLDER_CATALOG,
    "data_domain": "gl_accounting",
    "delivery": "daily",
    "domain": "finance",
    "job_name": "abs_daily",
    "layer": "bronze",
    "notify": {"on_failure": ["owner@example.com"], "on_success": []},
    "pipeline_id": "abs_accounting",
    "quarantine": {
        "classification": "internal",
        "schema": "abs_bronze",
        "table": "finance_abs_qrntn",
    },
    "raw_to_bronze": {
        "bronze": {"classification": "internal", "schema": "abs_bronze", "table": "finance_abs"},
        "columns": [],
        "dedup": False,
        "primary_keys": [],
        "rules": [],
    },
    "requested_by": "fdh",
    "source_to_raw": {
        "method": "volume_files",
        "source": {
            "delimiter": ",",
            "format": "csv",
            "header": True,
            "path": f"/Volumes/{PLACEHOLDER_CATALOG}/abs_raw/finance_source_extract/",
            "path_glob": "*.csv",
            "system": "abs",
        },
    },
    "spec_version": 1,
    "team": "fdh",
    "technical_owner": "owner@example.com",
}


def test_with_catalog_rewrites_only_placeholder_volume_paths():
    document = {
        "path": f"/Volumes/{PLACEHOLDER_CATALOG}/raw/vol/",
        "other": f"/Volumes/{PLACEHOLDER_CATALOG}x/raw/vol/",
        "list": [f"/Volumes/{PLACEHOLDER_CATALOG}/a/b/", "keep"],
        "count": 3,
    }
    assert _with_catalog(document, "prod_cat") == {
        "path": "/Volumes/prod_cat/raw/vol/",
        "other": f"/Volumes/{PLACEHOLDER_CATALOG}x/raw/vol/",
        "list": ["/Volumes/prod_cat/a/b/", "keep"],
        "count": 3,
    }


def test_manifest_load_points_the_raw_volume_at_the_environment_catalog(tmp_path):
    manifest = tmp_path / "abs_daily.json"
    manifest.write_text(
        json.dumps({"pipelines": [{"pipeline_id": "abs_accounting", "config": DOCUMENT}]}),
        encoding="utf-8",
    )
    config = _configs_from_manifest(manifest, "aal_prod")["abs_accounting"]
    assert config.catalog == "aal_prod"
    assert config.path == "/Volumes/aal_prod/abs_raw/finance_source_extract/"
    assert config.table_name("bronze").startswith("aal_prod.")
