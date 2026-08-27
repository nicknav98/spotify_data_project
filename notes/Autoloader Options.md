#ingestion 

Schema Evolution Table - how different options take in new columns 

.cloudFiles.schemaEvolutionMode:

| Mode               | Behavior on reading new column                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `addNewColumns`    | Stream fails. New columns are added to the schema.                                                                                   |
| `rescue` (default) | Schema is never evolved and the stream does not fail due to schema changes. All new columns are recorded in the rescued data column. |
| `failOnNewColumns` | Stream fails and does not restart unless the provided schema is updated or the offending data file is removed.                       |
| `none`             | Does not evolve the schema; new columns are ignored and data is not rescued. Stream does not fail due to schema changes.             |



