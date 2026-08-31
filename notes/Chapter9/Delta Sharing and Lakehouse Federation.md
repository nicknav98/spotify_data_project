#governance #sharing #federation

## Why this exists (exam guide Section 4: Data Sharing and Federation)

Two distinct problems, two distinct features:
- **Delta Sharing / OpenSharing** — let *other people* (other Databricks workspaces, or literally anyone on any platform) query your live Delta tables without copying the data.
- **Lakehouse Federation** — let *you* query data that lives in someone else's system (Postgres, Snowflake, Salesforce, etc.) without copying it into Databricks first.

Both avoid data duplication; they just point in opposite directions.

---

## Delta Sharing — now branded "OpenSharing"

- The open protocol is built into Unity Catalog. Data is shared **live** — recipients query the current state of the table directly, no export/copy/ETL step. This is the "Use Delta Share to share live data from Lakehouse to any computing platform" objective.
- Two protocol flavors, chosen based on who the recipient is:

### D2D — Databricks-to-Databricks

- For recipients who **also** run a Unity Catalog–enabled Databricks workspace.
- Recipient creates a **catalog from the share**, then queries it like any other UC catalog — Catalog Explorer, CLI, or SQL. Access is **read-only**.
- **No token management for the provider.** Auth is handled entirely through Databricks/UC — this is the detail Q5 was testing. For performance, "history sharing" uses temporary, scoped-down cloud storage credentials so reads perform close to native access (falls back to pre-signed URLs in some cross-region/GovCloud cases).
- Shareable asset types: tables, views, **volumes**, and **notebooks**.
- Source: [D2D OpenSharing — Databricks on AWS](https://docs.databricks.com/aws/en/opensharing/share-data-databricks)

### D2O — Databricks-to-Open

- For recipients on **any platform**, not just Databricks — "any user on any computing platform, anywhere."
- Two auth options, and this time credentials *are* the provider's responsibility:
  - **Bearer token** — Databricks generates a credential file + activation link the provider distributes to the recipient. Databricks recommends configuring token expiry.
  - **OIDC federation** — recipient authenticates through their own IdP, no credential file to manage.
- Also supports a **directory-based access mode**: Databricks hands back the table's cloud storage location plus temporary cloud credentials, letting eligible recipients read straight from cloud storage.
- Source: [D2O OpenSharing — Databricks on AWS](https://docs.databricks.com/aws/en/opensharing/share-data-open)

### D2D vs. D2O — the exam-relevant contrast

| | D2D | D2O |
|---|---|---|
| Recipient platform | Must be UC-enabled Databricks | Any platform |
| Token management | None — handled by Databricks/UC | Provider manages tokens (or OIDC) |
| Performance | Near-native via history sharing | Pre-signed URLs / direct cloud creds |
| Shareable assets | Tables, views, volumes, notebooks | Tables/views (platform-agnostic subset) |

**Rule of thumb for exam scenarios:** "partner also has Databricks/UC" + "don't want to manage tokens" → **D2D**. "Partner uses [Snowflake/Power BI/pandas/anything else]" → **D2O**.

**Caveat from the Row Filters/Column Masks note:** table-level row filters/column masks *cannot* be shared via OpenSharing at all (unless they're ABAC-based, which can be shared under conditions) — worth cross-checking [[Dynamic Views]] if a question combines sharing + PII masking.

---

## Lakehouse Federation

- "Lakehouse Federation is the Databricks query federation platform" — it's the umbrella name; **query federation** and **catalog federation** are its two modes.
  - **Query federation** — register an external relational system (Postgres, MySQL, SQL Server, Snowflake, Redshift, Oracle, Salesforce Data 360, etc.) as a UC **foreign catalog**. Queries are pushed down (predicate/filter pushdown) to the source system at execution time; nothing is copied into Databricks.
  - **Catalog federation** — directly access data already sitting in another catalog platform's object storage, governed by that platform's catalog.
- **Governance:** because foreign catalogs are still UC objects, you get UC's fine-grained access control, lineage, and search over federated data — same governance model as native Delta tables, applied to external systems. This is the "Configure Lakehouse Federation with proper governance across the supported source Systems" objective.
- Access is **read-only** from the federated source.
- Source: [What is Lakehouse Federation? / query federation — Databricks](https://docs.databricks.com/gcp/en/query-federation), [What is query federation? — Databricks on AWS](https://docs.databricks.com/aws/en/query-federation/database-federation)
