# Spec profiles

Shared defaults for table specs. A spec or table-group file sets:

```yaml
extends: profiles/db_merge_confidential
```

**Precedence (highest wins):** table entry → profile (`extends`) → `source.yaml` / folder → framework default.

Profiles are YAML fragments merged under the table document before validation. Put repeated `data.classification`, `notify`, `raw_to_bronze.dedup`, and similar here — not in every table file.
