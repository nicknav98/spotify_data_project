# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver layer: dedupe streaming bronze_artists, join against static bronze_albums

# COMMAND ----------

import pyspark.sql.functions as F

ARTISTS_TABLE = "`spotify-data-project-dev`.`default`.bronze_artists"
ALBUMS_TABLE = "`spotify-data-project-dev`.`default`.bronze_albums"
SILVER_TABLE = "`spotify-data-project-dev`.`default`.silver_artist_albums"

CHECKPOINT_PATH = (
    "/Volumes/spotify-data-project-dev/default/"
    "checkpoints/silver_artist_albums"
)

# COMMAND ----------

# MAGIC %md
# MAGIC bronze_artists carries duplicate artist_ids across versioned CSV
# MAGIC drops (v1, v2, ...) -- confirms why the watermark + dropDuplicates
# MAGIC below is needed rather than a plain join.

# COMMAND ----------

raw_count = spark.read.table(ARTISTS_TABLE).count()
distinct_count = spark.read.table(ARTISTS_TABLE).select("artist_id").distinct().count()
print(f"{raw_count} rows, {distinct_count} distinct artist_id")

# COMMAND ----------

ARTIST_COLUMNS = [
    "artist_id",
    "name",
    "country_of_origin",
    "career_start_year",
    "primary_genre_l1",
    "primary_genre_l2",
    "spotify_followers",
    "spotify_popularity",
    "is_solo_artist",
]

ALBUM_COLUMNS = [
    "artist_id",
    "album_id",
    "title",
    "album_type",
    "release_year",
    "release_date",
    "track_count",
    "label",
]


def process_silver_artist_albums():
    deduped_artists = (
        spark.readStream.table(ARTISTS_TABLE)
        .withWatermark("timestamp_added", "30 seconds")
        .dropDuplicates(["artist_id"])
        .select(*ARTIST_COLUMNS)
    )

    albums = spark.read.table(ALBUMS_TABLE).select(*ALBUM_COLUMNS)

    silver_df = (
        deduped_artists.join(albums, on="artist_id", how="inner")
        .withColumn("silver_ingested_at", F.current_timestamp())
    )

    query = (
        silver_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(SILVER_TABLE)
    )

    query.awaitTermination()


process_silver_artist_albums()
