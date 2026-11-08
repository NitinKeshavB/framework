# lakeflow_framework

Config-driven Lakeflow file ingestion. Pin this wheel and supply one bronze YAML file per pipeline.

```python
from lakeflow_framework import build_pipeline

build_pipeline("billing_invoices")
```

Validate before a cluster starts:

```bash
lakeflow-framework validate configs/bronze
```

`databricks bundle deploy` builds this package. Jobs and pipelines reference `dist/*.whl`. The repository overview is the root `README.md`. The YAML contract is `framework_docs/README.md`. Sample bronze configs and tests are in `framework_examples/consumer`.
