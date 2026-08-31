#security #privacy #delta-lake

## What is a dynamic view?

A view whose output changes per querying user, based on their identity/group membership — `is_member('group_name')` checks whether the current user belongs to a workspace group at query time.

- **Column masking** — redact specific columns unless the caller is privileged.
- **Row filtering** — restrict which rows are returned unless the caller is privileged.
- The underlying table is never modified; masking/filtering happens live, per query, in the view definition.

## Column masking

```sql
CREATE OR REPLACE VIEW customers_vw AS
  SELECT
    customer_id,
    CASE WHEN is_member('admins_demo') THEN email      ELSE 'REDACTED' END AS email,
    gender,
    CASE WHEN is_member('admins_demo') THEN first_name ELSE 'REDACTED' END AS first_name,
    CASE WHEN is_member('admins_demo') THEN last_name  ELSE 'REDACTED' END AS last_name,
    CASE WHEN is_member('admins_demo') THEN street      ELSE 'REDACTED' END AS street,
    city,
    country,
    row_time
  FROM customers_silver
```

- Non-admins get `REDACTED` for every PII column; admins see the real values.
- `customer_id`, `gender`, `city`, `country`, `row_time` are treated as non-sensitive and left unmasked for everyone.

## Row filtering (layered on top of a masked view)

```sql
CREATE OR REPLACE VIEW customers_fr_vw AS
SELECT * FROM customers_vw
WHERE
  CASE WHEN is_member('admins_demo') THEN TRUE
       ELSE country = 'France' AND row_time > '2022-01-01'
  END
```

- Non-admins only see French customers from after a cutoff date; admins see everything.
- Views can be **layered**: `customers_fr_vw` is built on `customers_vw`, so it inherits the column masking automatically — row filtering and column masking compose without repeating the `CASE` logic.

## ⚠️ N.B. — dynamic views only protect the view, not the table

- Masking is enforced by the **view definition**, not the underlying data. Anyone granted direct `SELECT` on `customers_silver` (or the catalog path/files) bypasses the redaction entirely.
- **Change Data Feed bypasses the view.** `table_changes(...)` / `readChangeFeed` reads from the base table's change log, not through `customers_vw` — a non-admin (or any downstream CDC consumer) reading the change feed sees unmasked PII regardless of what the view exposes. See [[Propagating Deletes]].
- Grants should be structured so non-privileged users/roles only ever get `SELECT` on the *view*, never on the base table or its change feed.
- Deleted rows raise the same issue as in [[Propagating Deletes]]: a masked view stops showing a "deleted" customer's row, but the PII isn't gone until `VACUUM` runs on the base table — dynamic views are an access-control layer, not a data-retention/erasure mechanism.

## ⚠️ N.B. — streaming tables vs. materialized views hit the same stale-join mechanic

- [[Propagating Deletes]]'s stream-static join section notes that updates to the static/dimension table do **not** recompute output already produced by earlier microbatches — a **streaming table** joined against a dimension locks in whatever the dimension looked like at process time ("fast-but-wrong").
- A **materialized view** re-evaluates on refresh and stays consistent with the *current* state of all source tables, including dimensions — so a retroactive correction (e.g. a customer address fix) is reflected even for rows processed before the fix.
- Don't confuse a materialized view with a SQL temp view: an MV is a persisted, Unity Catalog–managed, queryable LDP object refreshed on a schedule/trigger — not ephemeral. SCD Type 1/2 storage (via AUTO CDC, see [[AUTO CDC APIs]]) is a separate mechanism from this streaming-table-vs-MV choice.
- Source: [Streaming tables — Databricks on AWS](https://docs.databricks.com/aws/en/ldp/concepts/streaming-tables)
