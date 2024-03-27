from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.functions import monotonically_increasing_id
from SpotifyAPI import SpotifyAPI

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Define new object with API methods to process Spotify data

sp = SpotifyAPI()


def process_audio_analysis(tracks_ids: list):
    # tracks = self.process_single_row_objects("audio-analysis", tracks_ids)
    rows = []
    for track_id in tracks_ids:
        audio_analysis_data = sp.process_single_row_objects("audio-analysis", tracks_ids)
        row = Row(track_id=track_id, **audio_analysis_data.first().asDict())
        rows.append(row)

    df = spark.createDataFrame(rows)

    processed_df = df.select(
        F.col("track_id"),
        F.col("key"),
        F.col("num_samples"),
        F.col("tempo"),
        F.col("loudness")
    )

    return processed_df


def process_playlists(playlist_id: str):
    playlists = sp.parse_json("playlists", f"{playlist_id}/tracks")["items"]

    return sp.read_json_to_df(playlists)


if __name__ == "__main__":
    # Prepare playlist dataframe
    playlist_id = "4IAGMLxu1OLYgj27083hvY"
    playlist_data = process_playlists(playlist_id)

    artists = (
        playlist_data
        .select(
            F.col('track.artists.id').alias("artist_id"),
            F.col('track.artists.name').alias("artist_name")
        )
    )

    artists_with_ids = artists.withColumn("id", monotonically_increasing_id())
    artists_id_exploded = artists_with_ids.selectExpr("id", "posexplode(artist_id) as (id_index, artist_id)")
    artists_name_exploded = artists_with_ids.selectExpr("id", "posexplode(artist_name) as (name_index, artist_name)")

    artists_matched = artists_id_exploded.join(artists_name_exploded,
                                               (artists_id_exploded["id"] == artists_name_exploded["id"]) & (
                                                artists_id_exploded["id_index"] == artists_name_exploded["name_index"]))

    artists_matched = artists_matched.select("artist_id", "artist_name")

    playlists = (
        playlist_data
        .select(
            F.col("track.album.album_type"),
            F.col("track.album.id").alias("album_id"),
            F.col("track.album.name").alias("album_name"),
            F.col("track.album.release_date"),
            F.col("track.album.total_tracks"),
            F.col("track.artists.id").alias("artist_id"),
            F.col("track.artists.name").alias("artist_name"),
            F.col("track.duration_ms"),
            F.col("track.id").alias("track_id"),
            F.col("track.name").alias("track_name"),
            F.col("track.popularity")
        )
    )
