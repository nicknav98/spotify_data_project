#governance #ingestion

## What is it?

- **Lakehouse Federation** lets Databricks query external data sources **live**, in place, with no data migration/copy step
- The mirror image of [[Delta Sharing]] — Delta Sharing lets *others* query your live data; Federation lets *you* query someone else's live data (see [[Delta Sharing and Lakehouse Federation]])
- Two modes:
  - **Query federation** — register an external relational system (Postgres, MySQL, SQL Server, Snowflake, Redshift, Oracle, BigQuery, Salesforce Data 360, etc.) as a UC **foreign catalog**; queries are pushed down to the source system at execution time
  - **Catalog federation** — query data already sitting in another catalog platform's object storage (e.g. AWS Glue, Snowflake), using only Databricks compute — generally more cost/performance efficient than query federation

![[assets/Pasted image 20260831101315.png]]

- Source: [What is query federation? — Databricks on AWS](https://docs.databricks.com/aws/en/query-federation/database-federation)

## Ingestion vs. federation

- Databricks recommends **ingestion** (e.g. via Lakeflow Connect) for most use cases — it scales better for high data volumes, low-latency querying, and workloads constrained by third-party API rate limits
- Ingestion creates a **duplicate copy** of the data, which can become **stale** over time
- Use **federation** instead when avoiding that copy matters more than raw performance: ad hoc reporting, proof-of-concept work, or live/exploratory access to operational data in an external database
- For live data from another *Databricks* workspace specifically, prefer [[Delta Sharing]] over ingestion — no staleness, no duplication
- Source: [What is Lakeflow Connect? — Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/overview)

## Governance

- Foreign catalogs are still Unity Catalog objects → same fine-grained access control, lineage, and search as native Delta tables, applied to external systems
- Access is **read-only** (the one exception: federating a workspace's own legacy Hive metastore)

## Limitations

- **Pushdown coverage varies by source** — complex queries may not fully push down, so performance can fall back to the remote system's own compute instead of Databricks'
- Databricks **result/disk caching is not supported** for federated queries
- Large result sets risk **executor out-of-memory** errors on the Databricks side while materializing remote data
- Table/schema names are lowercased and UC-incompatible identifiers are dropped — possible name collisions
- Source: [What is query federation? — Databricks on AWS](https://docs.databricks.com/aws/en/query-federation/database-federation)
