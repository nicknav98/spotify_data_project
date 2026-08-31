#governance #sharing

## What is it?

- **Delta Sharing** is an open protocol for secure data sharing — recipients query **live** data directly, regardless of their platform, with no export/copy step
- Two protocol flavors, chosen based on who the recipient is

![[assets/Pasted image 20260831094254.png]]

## D2D — Databricks-to-Databricks

- For recipients who also run a Unity Catalog–enabled Databricks workspace
- Shareable assets: **tables, views, volumes, and notebooks**
- Recipient creates a **catalog from the share** and queries it like any other UC catalog — **read-only**
- No token management for the provider — auth is handled entirely through Databricks/UC via a sharing identifier tied to the recipient's metastore

## D2O — Databricks-to-Open

- For recipients on **any computing platform**, not just Databricks
- Shares tables registered in a UC metastore
- Provider manages recipient credentials — two options:
  - **Bearer token** — Databricks generates a credential file + activation link the provider distributes to the recipient
  - **OIDC federation** — recipient authenticates through their own identity provider
- Directory-based access mode (eligible tables): Databricks hands back the table's cloud storage location plus temporary cloud credentials for direct reads; ineligible tables fall back to pre-signed URLs

## History sharing

```sql
ALTER SHARE <share_name> ADD TABLE <table_name> WITH HISTORY;
```

- Lets D2D recipients run **time travel** queries and **streaming reads** against the shared table
- To let recipients query the table's **change data feed** via `table_changes()`, CDF must be enabled on the source table *before* it's shared `WITH HISTORY`
- Uses temporary cloud storage credentials scoped to the table's root directory → read performance close to native/direct access
- ⚠️ The performance benefit does **not** apply to partitioned tables
- Default behavior on DBR/DBSQL 16.2+; the older `WITH CHANGE DATA FEED` clause is deprecated in favor of `WITH HISTORY`
- Source: [ALTER SHARE — Databricks on AWS](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-share)

## Roles

- Creating/managing shares requires being a **metastore admin** or holding the **CREATE SHARE** privilege on the metastore
- Workspace admins get `CREATE SHARE` (and `CREATE RECIPIENT`) on the metastore by default when UC is auto-enabled

## Costs

- Delta Sharing requires no data replication to share
- **No egress cost** for reads within the same cloud region
- Cross-cloud or cross-region reads incur the cloud vendor's own data egress fees
- Mitigations: `DEEP CLONE` the data into a replica in the recipient's region, or share from **Cloudflare R2** (zero egress fees)
- Source: [Monitor and manage OpenSharing egress costs — Databricks on AWS](https://docs.databricks.com/aws/en/opensharing/manage-egress)

## Limitations

- **Read-only** — no write/update privileges can be granted on a Delta Sharing catalog or its objects
- Data must be in Delta table format (Iceberg tables can be shared via UniForm and are read back as Delta)
- Source: [What is OpenSharing? — Databricks on AWS](https://docs.databricks.com/aws/en/opensharing/)

See also: [[Delta Sharing and Lakehouse Federation]] for the D2D vs. D2O exam-focused comparison.
