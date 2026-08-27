#cdc #ldp #streaming

## What is it?

- **AUTO CDC** simplifies change data capture (CDC) processing in [[Lakeflow Spark Declarative Pipelines]]
- Replaces hand-written `MERGE INTO` statements for applying inserts/updates/deletes from a CDC source
- Supports **SCD Type 1** (overwrite with latest value) and **SCD Type 2** (retain full history)

## SQL syntax

```sql
CREATE FLOW flow_name AS
AUTO CDC INTO target_table
FROM STREAM(cdc_source_table)
KEYS (key_field)
APPLY AS DELETE WHEN operation_type = "DELETE"
SEQUENCE BY sequence_field
COLUMNS * EXCEPT (operation_type, sequence_field)
STORED AS SCD TYPE 1;
```

## SEQUENCE BY

- Specifies the logical order of CDC events in the source data
- Used to automatically handle out-of-sequence records — a late-arriving change with an older sequence value is not applied over a newer one
- Sequencing on multiple columns:

```sql
SEQUENCE BY STRUCT(operation_timestamp, operation_number)
```

- `AUTO CDC INTO` creates two backing data structures:
  - The **target** table/view — what users query
  - A hidden `__apply_changes_storage_<target_table>` table — stores extra bookkeeping (e.g. tombstones) needed to resolve out-of-order data

## Python syntax

```python
create_auto_cdc_flow(
    target = "target_table",
    source = "cdc_source_table",
    keys = ["key_field"],
    sequence_by = col("operation_date"),
    apply_as_deletes = expr("operation_type = 'DELETE'"),
    except_column_list = ["operation_type", "operation_date"],
    stored_as_scd_type = 1
)
```

## OLD SYNTAX on DLT views

- Legacy DLT named the same functionality **APPLY CHANGES**; the code below is equivalent to `AUTO CDC INTO` / `create_auto_cdc_flow` above, just under the old names

**SQL**

```sql
APPLY CHANGES INTO target_table
FROM stream(cdc_source_table)
KEYS (key_field)
APPLY AS DELETE WHEN operation_type = "DELETE"
SEQUENCE BY operation_date
COLUMNS * EXCEPT (operation_type, operation_date)
STORED AS SCD TYPE 1;
```

**Python**

```python
apply_changes(
    target = "target_table",
    source = "cdc_source_table",
    keys = ["key_field"],
    sequence_by = col("operation_date"),
    apply_as_deletes = expr("operation_type = 'DELETE'"),
    except_column_list = ["operation_type", "operation_date"]
)
```
