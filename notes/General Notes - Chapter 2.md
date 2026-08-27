#deduplication #ingestion 

**SCD - Slowly changing dimensions** 

SCD Type 0 - Static / Append only look up tables, where data can not be overwritten 
SCD Type 1 - Rows can be updated, with old values not recorded, one to one matching on unique rows 
SCD Type 2 - Historical Changes captured on unique keys - extra columns to mark current row, start timestamp for row, and end_date for when the row has become obsolete'

**Water-marking** 

A watermark is Spark’s moving **“we have waited long enough”** line for event-time processing.

Kafka delivers records in offset/arrival order, but each message may describe an event that happened earlier. ==Watermarking== tells Spark how much disorder to tolerate before it finalizes old windows and clears their state.

Think of it as a train station’s lost-property desk:

- **Event time** = when the passenger lost the item.
- **Arrival time** = when the item reaches the desk via Kafka.
- **10-minute watermark** = “Keep the desk open for items up to 10 minutes late.”
- Once the watermark passes a window, Spark can emit the final result and discard its in-memory state; still-later records may be dropped.

With `withWatermark("event_time", "10 minutes")`, Spark approximately advances the watermark as:

```
maximum event time observed − 10 minutes
```

```
from pyspark.sql import functions as F

events = (
    spark.readStream.format("kafka")
      .option("subscribe", "orders")
      .load()
      # parse Kafka value and produce a timestamp column named event_time
)

counts = (
    events
      .withWatermark("event_time", "10 minutes")
      .groupBy(F.window("event_time", "5 minutes"))
      .count()
)
```

Important: this is not a Kafka offset or ingestion-time cutoff. It applies to the timestamp carried in the event, and principally bounds state for operations such as windows, stream-stream joins, and streaming deduplication. A larger delay accepts more late data but retains more state; a smaller one reduces state/latency but risks dropping valid late events.

References: [Spark Structured Streaming — late data and ==watermarking==](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#handling-late-data-and-watermarking), [Spark’s Kafka source integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).