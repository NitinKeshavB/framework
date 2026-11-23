# Quality rules for bronze tables

A rule is a SQL condition that every good row must satisfy. By default a row that fails any rule goes to the table's quarantine table instead of bronze. Declaring a rule (or a `quarantine:` block) is what creates that table; a spec with neither has no quarantine, and a row failing the framework's own checks is dropped and counted ([README](README.md#quality-rules-and-quarantine)). Every rule example on this page is in `framework_tests/test_quality_cookbook.py`, which checks that the validator accepts it and that sample values pass and fail as shown. The same samples were also run on Spark.

## What happens to a row that fails a rule

You choose with `on_fail`, set in **one place only**: on `raw_to_bronze` for every rule of the table, or on the rules themselves. A spec that sets it in both places is refused, so the action of a rule is always found in one place. `on_fail` needs `rules`: `raw_to_bronze.on_fail` with no rules is refused by `./run.sh generate`:

| Action | A row that fails | The load | Use it when |
| --- | --- | --- | --- |
| `quarantine` (default) | Goes to `<bronze table>_qrntn` with the names of the rules it failed. Good rows load | Continues, `COMPLETED` | You want the good data now and the bad data kept to look at |
| `drop` | Is discarded and kept nowhere. Only the counts in `expectations` show it | Continues | The bad rows are noise you do not need (it loses data, so use it sparingly) |
| `warn` | Loads into bronze like any row. The failure is only counted (`expectations`, the pipeline's data quality view) | Continues | A rule you are still tuning, or a soft expectation |
| `fail` | **Blocks the whole file.** None of the file's rows reach bronze, the good ones included. Its bad rows go to quarantine | The run is `FAILED`, naming the file and the rule. Every other file still loads | A file with one bad row is not trustworthy, and you would rather stop and fix it |

```yaml
# One action for the whole table (also used by the framework's own checks):
raw_to_bronze:
  on_fail: drop
  rules:
    - { name: ride_id_present, expression: "ride_id IS NOT NULL" }                    # drop
    - { name: end_station_id_length, expression: "..." }                               # drop

# Or one per rule (a rule that says nothing quarantines):
raw_to_bronze:
  rules:
    - { name: ride_id_present, expression: "ride_id IS NOT NULL" }                    # quarantine
    - { name: ended_not_before_started, expression: "...", on_fail: fail }             # stop the load
    - { name: end_station_id_length,    expression: "...", on_fail: warn }             # just count
```

Rules of different actions mix in one table. A row that fails a `quarantine` rule is quarantined even if it also failed a `warn` rule; a row that fails only `drop` and `warn` rules is dropped; a row that fails only `warn` rules loads.

**What `fail` does, for a table loaded from files.** A row that breaks a `fail` rule is kept out of bronze and the load is recorded `FAILED`, but the pipeline and the job do **not** fail:

- The bad row goes to quarantine, so you can see what was wrong. The **rest of its file loads** to bronze, as does every other file of the run.
- The file is `FAILED` in `ledger_detail` (`error_message`: `rows broke fail rule(s): <rules>`) and the table's `load_ledger` row is `FAILED` with `files_failed` set and the files and rules named. Watch `v_load_failures`, not only the job status.
- A table with no quarantine table keeps those rows nowhere. Its `load_ledger` row is still `FAILED`, naming the rule and the number of rows, but no file is marked in `ledger_detail`.
- **The file is marked as read.** The next run does not pick it up. To load its bad rows you fix the data or the rule and run a backfill (below).
- **A rule change applies from the next run.** What a row's rules mean is decided once, when the file is read, and stored. Files already read are never judged again by a changed rule, unless you backfill them.

**This changed.** A `fail` rule used to block its whole file (no row of it reached bronze) and fail the update and the job. It no longer does, because holding back a file's good rows needed a working table per pipeline.

A table loaded from a **database** has no files, so `fail` there stops the whole run: no row of the run, good ones included, reaches bronze.

The checks the framework adds itself (`schema_conformance`, `cast_<column>`, `not_null_<key>`) follow `raw_to_bronze.on_fail`, or quarantine when it is not set (drop, when the table has no quarantine). With `on_fail: fail` on `raw_to_bronze`, a value that does not fit its column, or an empty key, is treated as a `fail` row.

Not available yet: a limit such as "fail only when more than 5% of rows are bad".

### A file that did not come, or came empty

A scheduled delivery runs whether or not a file arrived, so a table in one **expects a file with rows each run** (`source.expect_file`, on by default for a scheduled delivery, off for one fired by a file arriving; set it per table to change that). The job always **succeeds**; the ledger says what the data did:

| What happened | `load_ledger` | `ledger_detail` |
| --- | --- | --- |
| No file arrived since the previous run | `FAILED`, reason `no file arrived in <folder> since the previous run`; every count is 0 | no row (there is no file to name) |
| A file arrived and has no rows (0 bytes, or a header and nothing else) | `FAILED`, reason `N file(s) arrived with no rows: <names>`; `files_failed` counts them | one row per file: `FAILED`, `file has no rows`, counts 0 |
| A file arrived with rows | as usual | as usual |

A run with a good file and an empty one loads the good one and still records the load `FAILED`. An empty file is reported once, in the run that found it, not again on the next run. "Since the previous run" means since the previous regular run started, so a backfill does not move it. This looks at the volume's file names, sizes and arrival times only; no file is read. If the pipeline's own ledger write is cut off at shutdown, the hourly catch-up records the run without this check.

### Reloading files: backfill

One job, `framework_backfill` (in `framework_bundle`), reloads any table. It loads the table's spec from the registry and runs the same staging step as the pipeline, so the same rules, rule actions, audit columns and ledger apply:

```
./run.sh backfill <target> <profile> table=abs_gl mode=files files=a.csv,b.csv dry_run=false
./run.sh backfill <target> <profile> table=abs_gl mode=dates date_from=2026-11-01 date_to=2026-11-03 dry_run=false
./run.sh backfill <target> <profile> table=abs_gl mode=failed dry_run=false          # every file whose latest status is FAILED
./run.sh backfill <target> <profile> table=abs_gl mode=full_refresh confirm=true dry_run=false
```

- It lists the files and changes nothing unless `dry_run=false`.
- `dates` are **arrival times in UTC**. A date alone is the start of its day, and an end date includes that whole day.
- With no files and no dates it reloads **every** file in the directory, and needs `confirm=true`.
- A reloaded file's earlier rows are removed from bronze and quarantine first, so a reload never leaves a duplicate. Running the same backfill twice gives the same data.
- A backfill records these as `FAILED` too, and the job still succeeds with a `WARNING:` line: a **file name that is not in the folder** (`file not found`), a file that was read and **has no rows** (`file has no rows`), and a **date range or folder with no file at all**. The files that do exist still load. (`mode=failed` with nothing failed is not an error: there is nothing to retry.) Give several names with commas: `files=a.csv,b.csv`.
- A file that still breaks a `fail` rule is recorded `FAILED` again: its bad rows go to quarantine and its good rows load. The backfill **goes on with the next file** and the job ends **successfully, with a `WARNING:` line** naming those files. The ledger row of the backfill is `FAILED`. A job has no "succeeded with warning" status, so look at the task output or `v_load_failures`.
- `full_refresh` refreshes the table's own pipeline with a full refresh of that table: it deletes the table's data and reads every file again under the current rules. It also resets the table's change feed history, so a silver table that reads it must be rebuilt. Files with `fail` rows are judged again, and fail again unless the rule or the file changed.
- Not supported by file: tables that keep history by key (`scd1`, `scd2`) and tables loaded from a database. Use `full_refresh` for those.

Each run's ledger row shows `rows_read ≈ rows_inserted + rows_quarantined + rows_dropped` (a backfill counts `rows_dropped` per file; a pipeline run takes it from the table's flows); a warning is logged if it does not.

## How a rule behaves

- **One rule, one condition.** The rule must be **true** for a good row. Anything else fails it, **including NULL**. A column that may be empty must say so: `phone IS NULL OR phone rlike '...'`.
- **Every rule runs on every row.** The row does not stop at the first failure. It carries the names of all the rules it failed in `failed_rules`, so name each rule for what it checks.
- **By default a failing rule does not fail the load.** The good rows load, the bad rows go to `<bronze table>_qrntn`, and the run shows `COMPLETED` with `rows_quarantined` set. Change that with `on_fail` (above).
- **A row is in bronze or in quarantine, never both.**
- **Rules are about one row.** They can look at that row's columns and nothing else (see "What a rule cannot do").

## Writing a rule

Rules go under `raw_to_bronze.rules` in the table's spec. Each has a `name` (letters, digits, underscore; unique in the table) and an `expression`:

```yaml
raw_to_bronze:
  rules:
    - { name: invoice_id_present, expression: "invoice_id IS NOT NULL AND length(trim(invoice_id)) > 0" }
    - { name: invoice_id_format,  expression: "invoice_id rlike '^INV-[0-9]{6}$'" }
    - name: amount_not_negative
      expression: "amount >= 0"
```

Put the whole expression in **double quotes** and use **single quotes** for text and patterns inside it. Commas, braces and brackets inside the quotes are fine.

Run `./run.sh generate`, then `./run.sh check:rules` (below).

## How you know a rule works

Three checks, each catching what the one before cannot:

| Check | Run by | Catches | Does not catch |
| --- | --- | --- | --- |
| **Text check** | `./run.sh generate` and `bundle:validate` (always) | A column the table does not have, an unapproved function or word, a bad quote, `;` or a comment, an unclosed or extra bracket, a rule that stops half way (`... AND`) | Anything that needs an engine to know |
| **Engine check** | `./run.sh check:rules <profile> <warehouse-id>` | A regex that does not compile, a function with the wrong number of arguments, a rule that is not a condition, any syntax error | A rule that is valid but wrong (see the next row) |
| **Data check** | `./run.sh check:rules <profile> <warehouse-id> <catalog>` | A rule that would quarantine most or all existing rows: always false, a mistyped value list, a regex that is legal but not what you meant | Rows not loaded yet |

The text check cannot know whether `rlike '^[0-9'` compiles, or whether `length(a, b)` is the right call. Those pass `generate` and then fail when the pipeline loads, and a rule that cannot be analysed fails the **whole update**, not just its own table. So run the engine check before every deploy that changes a rule.

```text
$ ./run.sh check:rules PROD <warehouse-id> aal_dev_fdh
abs/abs_gl.yaml: 26 rule(s)
  ok     ride_id_present (80,685 rows checked)
  ERROR  ride_id_format: [INVALID_PARAMETER_VALUE.PATTERN] The value of parameter(s) `regexp` in `rlike` is invalid: '^[0-9'.
  ok     start_station_name_present: would quarantine 3 of 80,688 rows
  ...
26 rule(s) checked, 1 with an error
```

It exits non-zero when any rule errors, so it can gate a deploy. It uses your Databricks CLI profile and any SQL warehouse you can run on. The engine check runs each rule on one sample row (a value of each column's declared type), so it needs no data. The data check also needs the bronze table to exist, and skips a table that does not.

Two things it cannot tell you: that a rule means what you intended, and rows it has not seen. A rule that is valid but wrong shows up in the data check as a rule that quarantines almost everything.

## Regex checks

Use `rlike` (or `regexp`, the same thing): `column rlike 'pattern'` is true when the pattern matches. The pattern is a Java regular expression.

**Anchor it.** `rlike` is true if the pattern matches *anywhere* in the value. Without `^` and `$`, `code rlike '[0-9]{3}'` accepts `abc123xyz`. For "the whole value looks like this" write `^...$`.

```yaml
- { name: invoice_id_format, expression: "invoice_id rlike '^INV-[0-9]{6}$'" }
```

| Value | Result |
| --- | --- |
| `INV-000123` | passes |
| `INV-12`, `xINV-000123`, `INV-000123x` | fail |
| empty (NULL) | fails: add `invoice_id IS NULL OR ...` if it may be empty |

**Avoid backslashes.** Spark treats `\` in a text literal as an escape and drops it, so `'\d+'` is read as `d+`. There is no error: `'123' rlike '\d+'` is simply **false**. `\\d` works, but YAML wants it doubled again, so a mistake is easy and silent. Use character classes instead:

| Instead of | Write |
| --- | --- |
| `\d` | `[0-9]` |
| `\.` (a dot) | `[.]` |
| `\s` (a space) | `[ ]` |
| `\w` | `[A-Za-z0-9_]` |
| `\+` | `[+]` |

**Case-insensitive:** put `(?i)` first: `status rlike '(?i)^(new|open|closed)$'`.

**Pull a part out:** `regexp_extract(value, 'pattern', group)` returns the group, or an empty text when nothing matches. `length(regexp_extract(code, '^([A-Z]{3})-[0-9]+$', 1)) = 3` means "starts with three capitals, a dash, then digits".

**Allowed characters only:** `length(regexp_replace(sku, '[^A-Z0-9]', '')) = length(sku)` is true when removing every character that is not a capital or digit removes nothing.

### Patterns you can copy

| Check | Rule |
| --- | --- |
| Digits only | `code rlike '^[0-9]+$'` |
| Number (negative and decimals allowed) | `lat rlike '^-?[0-9]+([.][0-9]+)?$'` |
| Fixed-length id | `ride_id rlike '^[0-9A-Fa-f]{16}$'` |
| Date as text, `2026-11-19` | `order_date rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` |
| Time as text, `2026-11-19 10:00:00` or with `T` | `started_at rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'` |
| Email | `email rlike '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'` |
| Phone, optional | `phone IS NULL OR phone rlike '^[+]?[0-9]{8,15}$'` |
| Postcode, four digits | `postcode rlike '^[0-9]{4}$'` |
| UUID, lower case | `request_id rlike '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'` |
| Contains no digit | `NOT name rlike '[0-9]'` |

A latitude held as text, `-90` to `90`, with a range check built into the pattern (a `|` inside a table cell breaks the table, so it is here):

```yaml
- { name: start_lat_in_range, expression: "start_lat rlike '^-?(([0-8]?[0-9])([.][0-9]+)?|90([.]0+)?)$'" }
```

### What the validator refuses in a rule

It reads the whole expression, **patterns included**:

- `;` and the comment markers `--` and `/*`: a pattern that needs `--` or `;` cannot be written.
- A **denied word** anywhere, even inside a pattern: `select`, `from`, `where`, `join`, `union`, `with`, `insert`, `update`, `delete`, `drop`, `table`, `values`, `set`, `add`, `remove`, `list`, `window`, `over`, `exists` and similar. `^(add|remove)$` is refused. Spell such a check with a character class, or list the values with `IN (...)`.
- A **column the table does not have**, or a function or type that is not approved (see the list below).

## Every kind of check you can add

All of these were run on Spark with values that must pass and values that must fail.

### Presence and shape

| Check | Rule | Passes | Fails |
| --- | --- | --- | --- |
| Not empty or blank | `invoice_id IS NOT NULL AND length(trim(invoice_id)) > 0` | `INV-1` | empty, spaces, NULL |
| Optional but not blank | `end_station IS NULL OR length(trim(end_station)) > 0` | NULL, `A` | spaces only |
| Length range | `length(name) BETWEEN 1 AND 10` | `Alice` | empty, 11 characters |
| No padding | `name = trim(name)` | `Alice` | ` Alice` |
| Upper case | `country_code = upper(country_code)` | `AU` | `au` |
| Starts with | `startswith(code, 'AU-')` | `AU-1` | `NZ-1` |
| Free text is clean | `note IS NULL OR NOT contains(note, 'TODO')` | NULL, `all good` | `TODO fix` |

Also approved: `endswith`, `contains`, `lower`, `ltrim`, `rtrim`, `left`, `right`, `substring`, `replace`, `concat`.

### Allowed values

| Check | Rule | Passes | Fails |
| --- | --- | --- | --- |
| One of a list | `status IN ('NEW', 'OPEN', 'CLOSED')` | `NEW` | `new`, `X` |
| One of a list, any case | `upper(status) IN ('NEW', 'OPEN', 'CLOSED')` | `new` | `x` |
| Not one of a list | `status NOT IN ('TEST', 'DUMMY')` | `NEW` | `TEST` |

### Numbers

Declare the column with a number type and the rule compares real numbers:

```yaml
columns:
  - { name: amount,   type: "decimal(18,2)", description: Invoice amount }
  - { name: quantity, type: int,             description: Units }
```

| Check | Rule | Passes | Fails |
| --- | --- | --- | --- |
| Not negative | `amount >= 0` | `0`, `10.5` | `-1`, NULL |
| In a range | `amount BETWEEN 0 AND 1000000` | `999999` | `1000001` |
| At most two decimals | `round(amount, 2) = amount` | `10.25` | `10.255` |
| Whole and positive | `quantity > 0` | `1` | `0` |
| Adds up across columns | `abs(line_total - quantity * unit_price) < 0.01` | `20.00 = 2 x 10.00` | `25.00` |

A value that cannot be read as the declared type goes to `_rescued_data` and the row fails the built-in `schema_conformance` rule. Also approved: `abs`, `ceil`, `floor`, `round`, `greatest`, `least`.

A number held as text (a column declared `string`) cannot be cast in a rule, because only the types `date` and `timestamp` are approved. Check its shape with a regex, as in the latitude example above, or declare the number type.

### Dates and times held as text

| Check | Rule | Passes | Fails |
| --- | --- | --- | --- |
| A real date | `try_cast(order_date AS date) IS NOT NULL` | `2026-11-19` | `2026-13-45`, `abc`, NULL |
| A real timestamp | `try_cast(started_at AS timestamp) IS NOT NULL` | `2026-11-19 10:00:00` | `nope` |
| Not in the future | `try_cast(order_date AS date) <= current_date()` | `2020-01-01` | `2999-01-01` |
| Year in a range | `year(try_cast(order_date AS date)) BETWEEN 2000 AND 2100` | `2026-01-01` | `1999-12-31` |
| Not too old | `datediff(current_date(), try_cast(order_date AS date)) <= 3650` | `2026-01-01` | `1990-01-01` |

Use `try_cast`, which gives NULL for a bad value, so the rule fails and the row is quarantined. Prefer it to `to_date` and `to_timestamp` for *checking* a value: whether those return NULL or raise an error on a bad value depends on the session's ANSI setting, and a rule must never stop the load. Also approved: `date_add`, `date_sub`, `date_diff`, `date_format`, `month`, `day`, `hour`, `minute`, `second`, `trunc`.

### Across columns and conditions

| Check | Rule | Passes | Fails |
| --- | --- | --- | --- |
| End not before start | `try_cast(ended_at AS timestamp) >= try_cast(started_at AS timestamp)` | end after start | end before start |
| Required when | `CASE WHEN status = 'CLOSED' THEN closed_at IS NOT NULL ELSE true END` | `OPEN` with no date; `CLOSED` with a date | `CLOSED` with no date |
| All or none | `(city IS NULL) = (postcode IS NULL)` | both empty, both set | one set |

The general shapes: `a IS NULL OR <check>` for an optional column, and `CASE WHEN <when> THEN <check> ELSE true END` for "only when".

### Approved words

- **Keywords:** `and or not is null true false in between like rlike regexp case when then else end as cast try_cast`
- **Functions:** `abs ceil ceiling coalesce concat contains current_date current_timestamp date date_add date_diff date_format date_sub datediff day endswith floor greatest hour if iff ifnull isnull least left length lower ltrim minute month nullif nvl nvl2 regexp_extract regexp_replace replace right round rtrim second size startswith substring timestamp to_date to_timestamp trim trunc upper year`
- **Types after `AS`:** `date` and `timestamp` only.

## Checks that are on without a rule

| Rule | Added when | Fails when |
| --- | --- | --- |
| `schema_conformance` | A CSV, delimited, JSON or text file | A value did not fit the declared columns, so it was rescued into `_rescued_data` |
| `cast_<column>` | A column is declared | The value cannot be cast to the declared type. For a `string` column this cannot fail |
| `not_null_<key>` | `primary_keys` is set | A key is empty |
| `not_null_<watermark_column>` | `scd1` or `scd2` | The ordering column is empty (a change cannot be ordered without it) |

## What a rule cannot do

A rule sees one row. It cannot:

- **Find duplicates or check uniqueness.** For a key, use `load_type: scd1` with `primary_keys`, which keeps the latest row per key.
- **Look at another table** (does this customer exist). Check it downstream, in silver, where both tables are available.
- **Aggregate** (the total is right, the file has enough rows) or use `SELECT`, subqueries or window functions.
- **Run your own functions.** Only the approved ones above.
- **Check a file as a whole** (it is empty, it is the wrong size). That is separate, file-level checking.

## Try one rule by hand

`./run.sh check:rules` does this for every rule. To try a single rule in the SQL editor, count how many rows of a table that already exists it would send to quarantine. The rule goes inside `NOT coalesce((...), false)`, which is exactly how the pipeline treats it:

```sql
SELECT count(*) AS total,
       count_if(NOT coalesce((start_station_id IS NOT NULL AND length(trim(start_station_id)) > 0), false)) AS would_fail
FROM <catalog>.abs_bronze.gl_abs;
```

To see which rows, filter with the same condition:

```sql
SELECT * FROM <catalog>.abs_bronze.gl_abs
WHERE NOT coalesce((start_station_id IS NOT NULL AND length(trim(start_station_id)) > 0), false) LIMIT 20;
```

Rules apply to files loaded from now on. Rows already in bronze are not re-checked unless the table is fully refreshed.

## Where to see the results

- **The rules:** `framework_meta.asset_registry_current`, the `pipeline` row for the table, `config:rules`. They are also in the spec and in `.build/manifests/<delivery>.json`.
- **Per run, per rule:** `framework_meta.load_ledger.expectations` (or `v_loads`) has each rule with `passed_records` and `failed_records`.
- **The rows and why:** `<bronze table>_qrntn`, with `failed_rules` listing the rules each row failed.
- **In the UI:** the pipeline (not the job), the latest update, the data quality view of the table's staging dataset.

```sql
SELECT e.name, e.passed_records, e.failed_records
FROM (SELECT explode(from_json(expectations,
        'array<struct<name:string,passed_records:bigint,failed_records:bigint>>')) AS e
      FROM <catalog>.framework_meta.v_loads WHERE dataflow = 'abs_gl')
WHERE e.failed_records > 0;
```

## Tips

- Name a rule `<column>_<what it checks>` (`ride_id_format`). The name is what shows up in `failed_rules`.
- Use two small rules instead of one `AND`: `failed_rules` then says exactly which part failed.
- Use `IS NULL OR` for every column that may legitimately be empty. Forgetting it quarantines every empty row.
- A quarantine table that fills up is a signal to look at the source, not a failure. Review `failed_rules` for the common cause.
