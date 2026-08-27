
Predictive Optimization is an AI-driven feature that automatically handles maintenance operations for Unity Catalog managed tables, eliminating the need for manual tuning, improving query performance, and reducing storage costs.

  

Predictive optimization runs the following operations automatically for enabled tables:

- `OPTIMIZE`* to triggers incremental clustering for enabled tables. If automatic liquid clustering is enabled, predictive optimization might select new clustering keys before clustering data.
    
- `VACUUM` to reduces storage costs by deleting data files no longer referenced by the table.
    
- `ANALYZE`: to triggers incremental update of table statistics. These statistics are used by the query optimizer to generate an optimal query plan. See [ANALYZE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-table).
    
* OPTIMIZE does not run ZORDER when executed with predictive optimization.

  

**Enable Predictive Optimization**

Predictive optimization is enabled by default for new accounts. To manually enable or disable predictive optimization for an account, navigate to Feature Enablement in your accounts console.

You can also enable or disable predictive optimization for a catalog, or a schema using:

`ALTER CATALOG <catalog_name> { ENABLE | DISABLE } PREDICTIVE OPTIMIZATION;`

`ALTER SCHEMA <schema_name> { ENABLE | DISABLE } PREDICTIVE OPTIMIZATION;`

  

Databricks recommends enabling predictive optimization for all Unity Catalog managed tables to simplify data maintenance and reduce storage costs.