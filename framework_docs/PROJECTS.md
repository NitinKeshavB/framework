# Curation: silver, gold, and data products

The built structure is described in [framework_curation/README.md](../framework_curation/README.md) and the `project.yaml` contract in [README.md](../README.md#projectyaml). This page records the design behind it and the part that is not built.

## Model

```
source system ──delivery──▶ bronze ──▶ silver ──▶ gold ──▶ product
domain-owned, shared                  project-owned in framework_curation
```

- A source system belongs to a domain. Many teams can add tables to it. Bronze is owned by the source system.
- A delivery is how a source's data arrives: a schedule, or files landing in the raw volume. One delivery is one ingestion pipeline and one job.
- Silver runs when the bronze or silver tables it is `triggered_by` have refreshed. Gold and data products wait on lower layers the same way.
- Users declare dependencies. They do not write cron times for silver, gold, or products.

## Ownership

| Object | Owned by | Recorded in the registry as |
| --- | --- | --- |
| Source system | One domain, named owner | `owner_kind = source_system`, `owner = <source system>` |
| Bronze table | The source system | the same, plus `requested_by` and `delivery` |
| Silver, gold, or data product | One project | `owner_kind = project`, `owner = <project>` |

A table is registered once. Two requests for the same file share one bronze table. Another project reads a silver, gold, or product as `<project>.silver.<name>` (or `.gold.` / `.product.`) and never creates it again. The checks are in three places: `generate` and `projects:generate` before merge, the collector at registration, and `v_ownership_conflicts` for anything that slipped through. See [END_TO_END.md](END_TO_END.md#ownership).

## What is built

- `project.yaml` with silver, gold, and `products`, `triggered_by`, `reads`, and deadlines
- Reference checks against the ingestion bundle and the other projects
- The lower-layers-only rule, unique table names across projects, and no loops between projects
- `PROJECT_MAP.md` per project and `PROJECTS_INDEX.md`
- `new-project` and the generated per-project `databricks.yml` with targets
- `bundle:validate` and `bundle:deploy` regenerate the maps first

## What is not built (phase 1)

- The transform for a silver, gold, or product table. There is no config for it yet.
- A pipeline and job generated per project from `project.yaml`.
- The trigger. The plan is a job `table_update` trigger with `ALL_UPDATED` on the tables a table waits for. It is untested on a workspace, as is its limit on watched tables.
- The deadline behaviour: at the deadline, `alert` or `run_stale`.

## Scenarios the design has to cover

| Scenario | Handling |
| --- | --- |
| One source, one delivery | Default. One trigger, one job. |
| A source with several cadences | Several deliveries in `source.yaml`; each table names one. |
| Files arrive one at a time but silver needs the full set | Trigger on a completion marker or gate on the expected files. Per-file arrival would start silver too early. Open question for phase 1. |
| Silver joins two sources | `triggered_by` lists both; it waits for both. |
| Gold across several projects | Same rule, across projects. |
| A table is read but should not wait | List it under `reads`. |
| An upstream is late | A deadline, with `alert` or `run_stale`. Fan-in without a deadline is flagged on the map. |
| Bronze fails | Nothing downstream fires, because no table updated. |
| Backfill or rerun | Downstream triggers fire again, so silver and gold must be idempotent. |
| A project reads another project's table | A three-part reference. The graph between projects must have no loops. |
| Streaming sources | Out of scope. |

## Risk of a shared delivery

One delivery's pipeline loads every team's tables. A broken spec from one team can fail the update for the others. Validation runs on every pull request and the source owner reviews contributions. If it matters for a busy source, a delivery can be split. That is not built.
