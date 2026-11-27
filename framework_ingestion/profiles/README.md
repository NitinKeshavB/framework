# Spec profiles

Shared defaults for table specs. A spec or table-group file sets:

```yaml
extends: profiles/db_merge_confidential
```

**Precedence (highest wins):** table entry → profile (`extends`) → `source.yaml` / folder → framework default.

Profiles are YAML fragments merged under the table document before validation. The path is from the bundle root (the folder holding `sources/` and `profiles/`), and `.yaml` may be left off. Nested mappings merge key by key; a list or a single value in the spec replaces the profile's. In a `tables:` file the profile applies to every entry. Put repeated `data.classification`, `notify`, `raw_to_bronze.dedup`, and similar here — not in every table file.
