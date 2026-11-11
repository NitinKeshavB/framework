# Projects: silver and gold

Each project owns its silver and gold tables. Bronze belongs to a source system in [framework_ingestion](../framework_ingestion/README.md), so a project reads bronze and never loads it.

```
framework_projects/
  PROJECTS_INDEX.md                       generated: every project and what it reads from other projects
  <domain>/<project>/
    project.yaml                          you edit this: silver and gold tables and what they depend on
    PROJECT_MAP.md                        generated: the map for this project
    databricks.yml                        the project's own bundle
```

## Ownership

- A silver or gold table belongs to one project. Another project may read it and may not create a table with the same layer and name. The check fails the build and names the owner.
- A project reads bronze by `<source_system>.<table>`. The table must exist in the ingestion bundle.
- To read another project's table, write `<project>.silver.<name>`. Projects must not depend on each other in a loop.
- A table may only depend on lower layers: bronze, then silver, then gold.

## project.yaml

```yaml
project: billing_reporting
domain: finance
owner: reporting.owner@jll.com

silver:
  customer_orders:
    triggered_by: [erp.customers, erp.orders]     # bronze tables
    deadline: { at: "07:00", on_miss: alert }
gold:
  revenue_daily:
    triggered_by: [silver.invoices_clean, silver.customer_orders]
    reads: [headcount_reporting.silver.headcount_clean]   # another project; never waited for
    deadline: { at: "08:00", on_miss: alert }
```

`triggered_by` is what fires the table's job: it runs once every listed table has refreshed. `reads` is read without waiting. A table with more than one `triggered_by` and no deadline is flagged on the map, because one late upstream would stall it. `on_miss` is `alert` or `run_stale`.

## Where does my new table go?

| I want | Do this |
| --- | --- |
| A new project | `./run.sh new-project <domain> <project> <owner-email>` |
| A silver table | Add it under `silver:` in your `project.yaml` |
| A gold table | Add it under `gold:` in your `project.yaml` |
| Data that is not in bronze yet | Ask for it in the ingestion bundle: `./run.sh new-table ...`. Then read it as `<source_system>.<table>`. |
| A table another project already makes | Read it as `<project>.silver.<name>`. Do not create it again. |

Then run `./run.sh projects:generate`. It checks every reference and rewrites the maps. `bundle:validate` and `bundle:deploy` do this first. `projects:check` fails if a map is out of date, for CI.

## The map

`PROJECT_MAP.md` shows each silver and gold table with what triggers it, what it reads, and its deadline. Upstream bronze tables are listed with their source system, delivery, and the team that requested them. It also lists checks, which projects use this project's tables, and a diagram.

## In the registry

A project's tables will be registered with `owner_kind = project` and `owner = <project>`, and the collector's one-owner check applies to them as it does to bronze. Bronze tables stay owned by their source system, so a project that reads `erp.orders` never owns it.

## Not built yet (phase 1)

- The silver and gold transforms. The framework does not yet build a pipeline or a job from these tables, so a project's `resources/` is empty and its `databricks.yml` only declares targets. This structure, the ownership checks, and the maps are in place so silver and gold can be added to it.
- Triggering a job from the tables it waits on (a table-update trigger), and the deadline behaviour. Both are untested on a workspace.

The design and the scenarios are in [framework_docs/PROJECTS.md](../framework_docs/PROJECTS.md), and the plan is in the [roadmap](../framework_docs/ROADMAP.md).
