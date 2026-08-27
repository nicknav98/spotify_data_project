#spark #optimisation #pandas

## What is a partition?
- A **partition** = subset of rows that share the same value in the column(s) a table is partitioned by
- Data is physically split into separate directories/files on disk, one per distinct partition value
- Enables **partition pruning**: queries that filter on the partition column skip scanning irrelevant files

![[Pasted image 20260824155600.png]]

## Creating a partitioned table
```sql
CREATE TABLE my_table (
  id INT,
  name STRING,
  year INT,
  month INT
)
PARTITIONED BY (year)
```

## Multi-column partitioning
- Partition by more than one column to create a nested directory structure (e.g. `year=2026/month=08/`)
```sql
CREATE TABLE my_table (
  id INT,
  name STRING,
  year INT,
  month INT
)
PARTITIONED BY (year, month)
```

![[Pasted image 20260824155917.png]]

## Choosing partition columns
- **Low cardinality** — few, recurring distinct values (e.g. year, region, category)
- **Size** — each partition should hold **at least ~1 GB** of data
- **Growth pattern** — good fit for columns where new values keep arriving indefinitely (e.g. datetime fields like `year`/`month`/`date`)

## Over-partitioning
- Too many small partitions hurts performance instead of helping it
- **Symptoms / risks:**
  - Increased storage costs and file count to scan (small tables partitioned unnecessarily)
  - Most partitions under 1 GB → sign of over-partitioning → slower query reads
- **Fix:** requires a **full rewrite** of all data files to re-partition — not a lightweight change

## Changing / removing partition boundaries
- Partition columns can't be altered in place — repartitioning means rewriting the underlying data files
- Typical approaches:
  - `CREATE TABLE ... AS SELECT` (CTAS) into a new table with the desired partitioning, then swap
  - `REPLACE TABLE ... PARTITIONED BY (...) AS SELECT ...`
  - Overwrite affected partitions with `dataframe.write.option("replaceWhere", "...")` for targeted rewrites

![[Pasted image 20260824160440.png]]
![[Pasted image 20260824160508.png]]
![[Pasted image 20260824160533.png]]
