# Databricks notebook source
# MAGIC %md
# MAGIC ## Read CSV files in volume, stream all artist csvs tagged with v*
# MAGIC


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define streaming bronze table, list of artists that could be updated every month 

# COMMAND ----------
import pyspark.sql.functions as F

SOURCE_PATH = (
    "/Volumes/spotify-data-project-dev/default/"
    "spotify-data-project/"
)

SCHEMA_PATH = (
    "/Volumes/spotify-data-project-dev/default/"
    "checkpoints/artists_schema"
)

CHECKPOINT_PATH = (
    "/Volumes/spotify-data-project-dev/default/"
    "checkpoints/write_artists_v3"
)


def process_bronze_artist():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "spotify_artists_*.csv")
        .option("header", "true")
        .load(SOURCE_PATH)
        .drop("_rescued_data")
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("timestamp_added", F.current_timestamp())
        .withColumn(
            "year_month",
            F.date_format("timestamp_added", "yyyy-MM")
        )
    )

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable("`spotify-data-project-dev`.`default`.bronze_artists")
    )

    query.awaitTermination()


process_bronze_artist()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronze_artists limit 10 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Set-up ALBUM static table 

# COMMAND ----------

def process_bronze_albums(): 
    df_albums = spark.read.csv(
        "/Volumes/spotify-data-project-dev/default/spotify-data-project/spotify_albums.csv",
        header=True,
        inferSchema=True
    ) \
        .withColumn("timestamp_added", F.current_timestamp()) \
        .withColumn("year_month", F.date_format("timestamp_added", "yyyy-MM"))
    
    df_albums.write \
        .mode("append") \
        .partitionBy("release_year") \
        .saveAsTable("`spotify-data-project-dev`.`default`.bronze_albums")
    
    print(f"Wrote {df_albums.count()} rows to bronze_albums")

process_bronze_albums()

# COMMAND ----------

