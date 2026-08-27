#spark #optimisation

## What is data file layout?
- The organization and storage structure of the underlying data files that make up a Delta table
- Optimizing the layout helps data-skipping algorithms find relevant files faster
- Main techniques: **Partitioning**, **Z-Ordering**, **Liquid Clustering**

## Partitioning — limitations
- Prevents file compaction across partition boundaries → magnifies the small-files problem
- Inefficient for **high-cardinality** columns → also causes small files
- **Static**: re-partitioning requires a full table rewrite

## Z-Ordering
- Groups similar data into co-located, optimized files **without** creating extra directories
- Effective for **high-cardinality** columns
- Not incremental — each run re-clusters from scratch

```sql
OPTIMIZE table_name ZORDER BY column_name
```

![[Pasted image 20260824162544.png]]
![[Pasted image 20260824162623.png]]

## Liquid Clustering
- Improved version of Z-Ordering — more flexible and more performant
- **Not compatible** with partitioning or Z-Ordering
- Clustering keys can be redefined without rewriting existing data
- Choose keys based on query patterns — table-dependent

**New tables:**
```sql
CREATE TABLE table1 (col1 INT, col2 STRING, col3 DATE)
CLUSTER BY (col1, col3)
```

**Existing tables:**
```sql
ALTER TABLE table2
CLUSTER BY (<clustering_columns>)
```

- After appending data, run `OPTIMIZE` to re-cluster:

```sql
OPTIMIZE table2
```

![[Pasted image 20260824162835.png]]

## Automatic Liquid Clustering
- Delegates key selection to Databricks, based on historical query patterns
- Requires **Predictive Optimization** on Unity Catalog managed tables

**New tables:**
```sql
CREATE TABLE table1 (col1 INT, col2 STRING, col3 DATE)
CLUSTER BY AUTO
```

**Existing tables:**
```sql
ALTER TABLE table2
CLUSTER BY AUTO
```
