#optimisation 

Deletion Vectors

Deletion vectors are a storage optimization feature you can enable on Delta Lake tables. By default, when a single row in a data file is updated or deleted, the entire Parquet file containing the record must be rewritten. With deletion vectors enabled for the table, `DELETE`, `UPDATE`, and `MERGE` operations create small files called "deletion vectors" to mark existing rows as removed or changed without rewriting the Parquet file. Subsequent reads on the table resolve the current table state by applying the deletions indicated by deletion vectors to the most recent table version.

  

So, deletion vectors indicate changes to rows as soft-deletes that logically modify existing Parquet data files in the Delta Lake table. These changes are applied physically when one of the following events causes the data files to be rewritten:

- An `OPTIMIZE` command is run on the table.
    
- Auto-compaction triggers a rewrite of a data file with a deletion vector.
    

  

**Enable deletion vectors**

Deletion vectors are enabled by default when you create a new table using a SQL warehouse or Databricks Runtime 14.1 or above.

To manually enable or disable support for deletion vectors on any Delta table or view, use the `delta.enableDeletionVectors` table property.ß