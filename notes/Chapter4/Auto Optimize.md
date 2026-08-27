#optimisation 

- Automatically compacts small files during individual writes to a Delta table
- Target file size: **128 MB** (not 1 GB)
- No Z-Ordering

## Two complementary features

- **Optimized writes** — attempts to write files at ~128 MB during the write itself
- **Auto compaction** — after a write completes, checks whether files can be further compacted; if so, runs an `OPTIMIZE` job targeting 128 MB (smaller than the 1 GB default target of a manual `OPTIMIZE`)

```sql
-- Enable manually on a table
ALTER TABLE table_name SET TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);
```

```python
# Enable for a Spark session
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

![[Pasted image 20260825093626.png]]

## ✅ Claude: is this on by default?

Yes, with caveats — behavior depends on table type, operation, and runtime:

- **`MERGE`, `UPDATE`, `DELETE`** — optimised writes and auto compaction are always on, cannot be disabled
- **`CTAS` / `INSERT`** — on by default when using SQL warehouses; for Unity Catalog **partitioned** tables, on by default from DBR 13.3 LTS+
- **Unity Catalog managed tables** (DBR 11.3 LTS+ / SQL warehouses) — Databricks auto-tunes file size in the background, no manual config needed
- **External / legacy tables** — not auto-tuned; optimised writes and auto compaction must be configured manually via the table properties above

**Autotuning target size scales with table size:** 256 MB for tables < 2.56 TB, scaling up to 1 GB for tables > 10 TB.

Source: [Control data file size — Databricks docs](https://docs.databricks.com/aws/en/delta/tune-file-size)

## N.B.

- Many small files isn't always a problem — it can improve data skipping and reduce rewrites during merges/deletes
- Databricks auto-tunes file size based on the workload operating on the table
- For frequent `MERGE` operations: optimized writes + auto compaction generate files smaller than 128 MB, reducing the duration of future merges
