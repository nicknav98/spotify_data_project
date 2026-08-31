from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE_PATH = (
    "/Volumes/spotify-data-project-dev/default/"
    "spotify-data-project/"
)

SCHEMA_PATH = (
    "/Volumes/spotify-data-project-dev/default/"
    "checkpoints/artists_schema"
)

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

@dp.table(
    name="`spotify-data-project-dev`.`default`.bronze_artists_SDP",
    partition_cols=["year_month"]
)
def bronze_artists():
    return (
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


@dp.materialized_view(
    name="`spotify-data-project-dev`.`default`.bronze_albums_SDP",
    partition_cols=["release_year"]
)
def bronze_albums():
    return (
        spark.read.csv(
            "/Volumes/spotify-data-project-dev/default/spotify-data-project/spotify_albums.csv",
            header=True,
            inferSchema=True
        )
        .withColumn("timestamp_added", F.current_timestamp())
        .withColumn("year_month", F.date_format("timestamp_added", "yyyy-MM"))
    )

ARTISTS_TABLE = "`spotify-data-project-dev`.`default`.bronze_artists_SDP"
ALBUMS_TABLE = "`spotify-data-project-dev`.`default`.bronze_albums_SDP"

@dp.table(
    name="`spotify-data-project-dev`.`default`.silver_artist_albums_SDP"
)
def silver_artists():
    deduped_artists = (
        spark.readStream.table(ARTISTS_TABLE)
        .withWatermark("timestamp_added", "30 seconds")
        .dropDuplicates(["artist_id"])
        .select(*ARTIST_COLUMNS)
    )

    albums = spark.read.table(ALBUMS_TABLE).select(*ALBUM_COLUMNS)

    return (
        deduped_artists.join(albums, on="artist_id", how="inner")
        .withColumn("silver_ingested_at", F.current_timestamp())
    )

SILVER_TABLE = "`spotify-data-project-dev`.`default`.silver_artist_albums_SDP"

@dp.materialized_view(
    name="gold_genre_performance_SDP",
    comment=(
        "Genre-level artist/album performance metrics, deduped to one row "
        "per artist_id before averaging so artists with more albums don't "
        "get overweighted."
    ),
)
def gold_genre_performance():
    silver = spark.read.table(SILVER_TABLE)

    artist_level = silver.select(
        "artist_id", "primary_genre_l1", "spotify_followers", "spotify_popularity"
    ).distinct()

    album_level = silver.groupBy("primary_genre_l1").agg(
        F.count("*").alias("album_count")
    )

    return (
        artist_level.join(album_level, on="primary_genre_l1", how="inner")
        .groupBy("primary_genre_l1", "album_count")
        .agg(
            F.countDistinct("artist_id").alias("artist_count"),
            F.round(F.avg("spotify_followers"), 0).alias("avg_spotify_followers"),
            F.round(F.avg("spotify_popularity"), 1).alias("avg_spotify_popularity"),
        )
        .select(
            "primary_genre_l1",
            "artist_count",
            "album_count",
            "avg_spotify_followers",
            "avg_spotify_popularity",
        )
    )
