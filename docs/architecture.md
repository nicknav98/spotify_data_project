# Ingestion architecture

A medallion layout for the two Spotify sources landing in the Unity
Catalog volume: artist snapshots dropped as versioned CSVs, and a single
album export. Bronze keeps each source's ingestion pattern honest, silver
is where they meet, and gold answers one question well instead of many
questions poorly.

```mermaid
flowchart TD
    A1["spotify_artists_v*.csv<br/>versioned drops → UC volume"]
    A2["spotify_albums.csv<br/>single file → UC volume"]

    subgraph BRONZE["BRONZE"]
        direction LR
        B1["Auto Loader (cloudFiles)<br/>.readStream · trigger(availableNow=True)"]
        B2["Batch read + write<br/>.read.csv(inferSchema) → .write.append"]
        T1[("bronze_artists<br/>+source_file, timestamp_added")]
        T2[("bronze_albums<br/>partitioned by release_year")]
    end

    subgraph SILVER["SILVER"]
        S1["silver_artist_albums<br/>artists ⋈ albums on artist_id<br/>dedupe · conform types"]
    end

    subgraph GOLD["GOLD"]
        G1[("gold_genre_performance<br/>artist/album counts, avg followers & popularity by genre")]
    end

    C1["BI dashboards / ad-hoc SQL"]

    A1 -->|new version dropped| B1
    B1 -->|writeStream + checkpoint| T1
    A2 -->|batch load| B2
    B2 -->|saveAsTable · append| T2
    T1 -->|join key: artist_id| S1
    T2 -->|join key: artist_id| S1
    S1 -->|GROUP BY primary_genre_l1| G1
    G1 --> C1
```

Two ingestion styles meet at silver: artists arrive incrementally through
Auto Loader as new versioned files land, albums arrive as a single batch
load. Silver joins them once on `artist_id`; gold is a plain aggregating
view on top, not another materialized copy.

## Layer reference

| Layer  | Object                   | Refresh                     | Grain                                            | Key logic                                                                |
|--------|--------------------------|------------------------------|---------------------------------------------------|---------------------------------------------------------------------------|
| Bronze | `bronze_artists`         | Streaming, `availableNow`    | 1 row per artist per source file                  | Schema inference + checkpoint; tags each row with `source_file`          |
| Bronze | `bronze_albums`          | Batch, append                 | 1 row per album                                   | Typed read, partitioned by `release_year`                                |
| Silver | `silver_artist_albums`   | Batch, after bronze lands     | 1 row per album, enriched with artist attributes  | Join on `artist_id`; dedupe; conform types; drop raw ingestion columns   |
| Gold   | `gold_genre_performance` | View, computed on read        | 1 row per genre                                   | Aggregates artist/album counts and average followers & popularity        |

Mirrors the current `static-tables` ingestion (Auto Loader for artists,
batch for albums) — silver and gold are the two layers this project
doesn't have yet.
