# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold layer: genre performance view over silver_artist_albums
# MAGIC
# MAGIC silver_artist_albums is one row per album, so spotify_followers and
# MAGIC spotify_popularity repeat once per album an artist has. Artist-level
# MAGIC metrics are deduped to one row per artist_id before averaging so
# MAGIC artists with more albums don't get overweighted; album_count is
# MAGIC taken from the ungrouped rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW `spotify-data-project-dev`.`default`.gold_genre_performance AS
# MAGIC WITH artist_level AS (
# MAGIC   SELECT DISTINCT
# MAGIC     artist_id,
# MAGIC     primary_genre_l1,
# MAGIC     spotify_followers,
# MAGIC     spotify_popularity
# MAGIC   FROM `spotify-data-project-dev`.`default`.silver_artist_albums
# MAGIC ),
# MAGIC album_level AS (
# MAGIC   SELECT
# MAGIC     primary_genre_l1,
# MAGIC     COUNT(*) AS album_count
# MAGIC   FROM `spotify-data-project-dev`.`default`.silver_artist_albums
# MAGIC   GROUP BY primary_genre_l1
# MAGIC )
# MAGIC SELECT
# MAGIC   a.primary_genre_l1,
# MAGIC   COUNT(DISTINCT a.artist_id) AS artist_count,
# MAGIC   al.album_count,
# MAGIC   ROUND(AVG(a.spotify_followers), 0) AS avg_spotify_followers,
# MAGIC   ROUND(AVG(a.spotify_popularity), 1) AS avg_spotify_popularity
# MAGIC FROM artist_level a
# MAGIC JOIN album_level al ON a.primary_genre_l1 = al.primary_genre_l1
# MAGIC GROUP BY a.primary_genre_l1, al.album_count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `spotify-data-project-dev`.`default`.gold_genre_performance
# MAGIC ORDER BY artist_count DESC
