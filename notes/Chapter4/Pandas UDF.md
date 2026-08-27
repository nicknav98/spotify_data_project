#spark #optimisation #pandas

## What is a Python UDF?
- A **User-Defined Function** lets you register custom Python logic as a function callable from DataFrame code or SQL
- Runs row-by-row — Spark ships each row's values out to a Python process, executes the function, and ships the result back

## Creating a Python UDF
**1. Plain Python function + `udf()`**
```python
def apply_discount(price, percentage):
    return price * (1 - percentage/100)

apply_discount_udf = udf(apply_discount)
```

**2. Decorator syntax**
```python
@udf("double")
def apply_discount_decorator_udf(price, percentage):
    return price * (1 - percentage/100)
```

**3. Register for use in SQL** — same function, callable from `%sql` or `spark.sql()`
```python
apply_discount_py_udf = spark.udf.register("apply_discount_sql_udf", apply_discount)
```

## Using a Python UDF
```python
from pyspark.sql.functions import col, lit

df_discounts = df_books.select("price", apply_discount_udf(col("price"), lit(50)))
```
```sql
SELECT price, apply_discount_sql_udf(price, 50) AS price_after_discount
FROM books_silver
```

## ⚠️ Serialization / deserialization cost
- Python UDFs run **outside the JVM**, in a separate Python process
- For every row: data is **serialized** (JVM → Python) and the result **deserialized** back (Python → JVM)
- Row-at-a-time execution also bypasses Catalyst's query optimizations and code generation
- Net effect: Python UDFs are typically the **slowest** option for transforming columns at scale

## Fix 1 — prefer native `spark.sql.functions`
- Built-in functions execute inside the JVM with Catalyst optimization — no serialization round-trip at all
- Same discount logic, no UDF needed:
```python
from pyspark.sql.functions import col, lit

df_discounts = df_books.select(
    "price",
    (col("price") * (lit(1) - lit(50) / 100)).alias("price_after_discount")
)
```
- Always check if a native function/expression can do the job before reaching for a UDF

## Fix 2 — use a Pandas (vectorized) UDF instead
- Backed by **Apache Arrow**: data is transferred to Python in **batches** as `pandas.Series`, not row-by-row
- Batching amortizes the serialization cost across many rows instead of paying it per row → much faster than a plain Python UDF, while still allowing arbitrary Python/pandas logic
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def vectorized_udf(price: pd.Series, percentage: pd.Series) -> pd.Series:
    return price * (1 - percentage/100)

df_discounts = df_books.select("price", vectorized_udf(col("price"), lit(50)))
```
- Can also be registered for SQL use:
```python
spark.udf.register("sql_vectorized_udf", vectorized_udf)
```
```sql
SELECT price, sql_vectorized_udf(price, 50) AS price_after_discount
FROM books_silver
```

## N.B.
- Preference order: **native `spark.sql.functions`** > **pandas UDF** > **plain Python UDF**
- Pandas UDFs still cross the JVM↔Python boundary — they just do it efficiently in batches; native functions avoid the boundary entirely
