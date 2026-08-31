#ldp #dlt #data-quality

## What is it?

- **Expectations** enforce data quality on tables built in [[Lakeflow Spark Declarative Pipelines]] by declaring constraints
- Each expectation names a **constraint** and a boolean **condition**; rows are checked against the condition as they're written
- Databricks always collects metrics on constraint violations (pass/fail row counts), regardless of which action is configured
- By default, violating records are **kept** — the action must be explicitly changed to drop or fail

## Violation actions

- **Warn** (default) — violating rows are kept in the table; the violation is only recorded in metrics
- **Drop** — violating rows are dropped before being written (`ON VIOLATION DROP ROW`)
- **Fail** — the entire update fails as soon as a violation is found (`ON VIOLATION FAIL UPDATE`), stopping the pipeline

| Action | SQL syntax | Python syntax |
|---|---|---|
| **Warn** (default) | `EXPECT (...)` | `dp.expect`, `dp.expect_all` |
| **Drop** | `EXPECT (...) ON VIOLATION DROP ROW` | `dp.expect_or_drop`, `dp.expect_all_or_drop` |
| **Fail** | `EXPECT (...) ON VIOLATION FAIL UPDATE` | `dp.expect_or_fail`, `dp.expect_all_or_fail` |

## SQL syntax

```sql
CREATE OR REFRESH STREAMING TABLE table_name (
  CONSTRAINT constraint_name EXPECT (condition) [ON VIOLATION {DROP ROW | FAIL UPDATE}]
) AS SELECT * FROM STREAM source
```

Example combining all three actions on one table:

```sql
CREATE OR REFRESH STREAMING TABLE my_table (
  CONSTRAINT recent_status EXPECT (status = 'ACTIVE' AND date >= '2025-01-01'),
  CONSTRAINT positive_value EXPECT (value > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS SELECT * FROM STREAM(source)
```

## Python syntax

```python
from pyspark import pipelines as dp

@dp.table
@dp.expect("recent_status", "status = 'ACTIVE' AND date >= '2025-01-01'")
@dp.expect_or_drop("positive_value", "value > 0")
@dp.expect_or_fail("valid_id", "id IS NOT NULL")
def my_table():
    return spark.readStream.table("source")
```

- Multiple `@dp.expect*` decorators can be stacked on the same table, mixing warn/drop/fail actions per constraint

## Multiple expectations at once

- `dp.expect_all`, `dp.expect_all_or_drop`, and `dp.expect_all_or_fail` apply a **single action** to a dictionary of constraints in one call — use these when several constraints should share the same violation action

```python
constraints = {
    "constraint_1": "condition_1",
    "constraint_2": "condition_2",
}

dp.expect_all(constraints)
dp.expect_all_or_drop(constraints)
dp.expect_all_or_fail(constraints)
```

- Mix `expect_all*` calls with individual `@dp.expect*` decorators when different constraints need different actions
