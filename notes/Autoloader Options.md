#ingestion

## Schema evolution — `cloudFiles.schemaEvolutionMode`

- Controls how Auto Loader reacts when a new column shows up in the source data

| Mode | Behavior on new column |
|---|---|
| `addNewColumns` | Stream fails with `UnknownFieldException`; new columns are added to the schema. Restarting the stream resumes processing with the updated schema. Existing columns do **not** have their data types evolved |
| `addNewColumnsWithTypeWidening` | Same as `addNewColumns`, but also widens supported data type changes (e.g. `int` → `long`); unsupported type changes go to the rescued data column |
| `rescue` | Schema is **never** evolved and the stream does **not** fail on schema changes — new columns are recorded in the rescued data column |
| `failOnNewColumns` | Stream fails and does **not** restart automatically — requires the provided schema to be updated (or the offending file removed) |
| `none` | Schema is not evolved; new columns are ignored and not rescued (unless `rescuedDataColumn` is separately set) |

## ⚠️ Default depends on whether a schema was provided

- **No schema provided** (relying on inference) → default is **`addNewColumns`**
- **Schema provided** → default is **`none`** — and `addNewColumns` isn't even allowed in this case, unless the schema is supplied as a *schema hint* rather than a full schema
- `rescue` is **not** the default in either case

Source: [Configure schema inference and evolution in Auto Loader — Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
