#dlt #ldp #streaming #etl

## What is it?

- LDP (Lakeflow [Spark] Declarative Pipelines) is a declarative ETL framework for building batch and streaming data pipelines with SQL and Python, powered by Apache Spark
- The evolution/rebrand of **Delta Live Tables (DLT)** — DLT is still required knowledge for the cert exam
- Extends **Spark Declarative Pipelines**, the open-source engine underneath (open sourced as of Spark 4.1); Databricks Runtime adds managed capabilities (orchestration, monitoring, serverless compute) on top

## Benefits

- **Automatic orchestration** — sequences steps with maximum parallelism, with progressive retry logic at the task, flow, and pipeline level
- Handles checkpoints, retries, and optimizations automatically
- Easy to implement CDC, SCD Type 1/2, and data quality controls (expectations) declaratively
- **Incremental processing** — an engine reprocesses only new/changed source data when maintaining a materialized view, rather than always recomputing from scratch

## Spark vs LDP

![[Pasted image 20260825135153.png]]

- **Plain Spark**: cannot create streaming tables in Spark SQL alone — must drop into PySpark to register a streaming table
- **LDP**: supports creating streaming tables directly in SQL via `CREATE OR REFRESH STREAMING TABLE`

## Creating LDP objects — SQL

```sql
-- Streaming table (append-only, incremental source)
CREATE OR REFRESH STREAMING TABLE table_name AS
SELECT * FROM STREAM read_files('/path/to/source');

-- Materialized view (batch, recomputed/refreshed)
CREATE OR REFRESH MATERIALIZED VIEW view_name AS
SELECT * FROM source_table;

-- Temporary view (not persisted/published)
CREATE TEMPORARY VIEW tv_name AS
SELECT * FROM source_table;
```

- The `STREAM` keyword marks a source as streaming — use it when reading into a streaming table, omit it for a materialized view
- Objects LDP can create: **streaming table**, **materialized view**, and **(temporary) view**

## Creating LDP objects — Python

```python
from pyspark import pipelines as dp

@dp.table
def table_name():
    # returns a streaming DataFrame -> creates a streaming table
    return spark.readStream.table("source_table")

@dp.materialized_view
def view_name():
    # returns a batch DataFrame -> creates a materialized view
    return spark.read.table("source_table")

@dp.temporary_view
def tv_name():
    return spark.read.table("source_table")
```

- `@dp.table` infers the object type from what the function returns: a **streaming** DataFrame produces a streaming table, a **batch** DataFrame produces a materialized view
- `@dp.materialized_view` explicitly forces a materialized view regardless of the query
- `@dp.temporary_view` creates a view that isn't published to the catalog — used for intermediate transformations within the pipeline
- The legacy `dlt` module (`import dlt`, `@dlt.table`, `@dlt.view`) still works, but Databricks recommends the newer `pyspark.pipelines` module (`dp`)

## Overview of object types

![[Pasted image 20260826091006.png]]

| Type | Behaviour |
|---|---|
| **Streaming table** | Processes an append-only source exactly once — ideal for ingestion and continuously growing data |
| **Materialized view** | Recomputed/refreshed to reflect the current state of its source — best for transformations consumed by multiple downstream tables |
| **(Temporary) view** | Evaluated on demand, never persisted or published — used for intermediate logic within the pipeline only |

## LDP vs DLT

![[Pasted image 20260826091059.png]]

> ⚠️ DLT is still required knowledge for the cert exam, even though LDP is the current name/API.

- **DLT (legacy)**: notebook-only development; `import dlt`; decorators `@dlt.table`, `@dlt.view`
- **LDP (current)**: supports source files (`.py` or `.sql`), not just notebooks; `from pyspark import pipelines as dp`
- Existing DLT pipelines are largely compatible and continue running under LDP without code changes

## Validating code

- **LDP** — source files support a **dry run**: resolves dataset/flow definitions and checks for problems (bad table/column names, config conflicts) without materializing or publishing any data
  - CLI: `databricks pipelines dry-run`
  - UI: pipeline details page → **Start** → **Dry run**; errors and incrementalization insights appear in the event tray
- **DLT** — notebook-only; the notebook's **Validate** button checks for syntax errors and tests pipeline logic without processing/appending any data


!!! NOTE !!!
**LDP has now been abbreviated to SDP** for exams certs