from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from SpotifyAPI import SpotifyAPI

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


sp = SpotifyAPI()


class APIMethods:

    def read_json_to_df(self, object):
        return spark.read.json(sc.parallelize([json.dumps(object)]))

    def process_single_row_objects(self, endpoint: str, objects: list):
        objects_to_process = []

        for obj in objects:
            objects_to_process.append(sp.parse_json(endpoint, obj))

        obj_dict = sc.parallelize(objects_to_process).map(lambda x: json.dumps(x))
        objs = spark.read.json(obj_dict)

        return objs

    # Definition for transformation of playlists
    def process_playlists(self, playlist_id):
        playlists = sp.parse_json("playlists", f"{playlist_id}/tracks")["items"]

        playlists = self.read_json_to_df(playlists)

        return (
            playlists
            .select(
                F.col("track.album.album_type"),
                F.col("track.album.id").alias("album_id"),
                F.col("track.album.name").alias("album_name"),
                F.col("track.album.release_date"),
                F.col("track.album.total_tracks"),
                F.explode(F.col("track.artists.id")).alias("artist_id"),
                F.col("track.artists.name").alias("artist_name"),
                F.col("track.duration_ms"),
                F.col("track.id").alias("track_id"),
                F.col("track.name").alias("track_name"),
                F.col("track.popularity")
            )
        )

    def process_audio_analysis(self, tracks_id: list):
        tracks = self.process_single_row_objects("audio-analysis", tracks_id)

        # Rozkminic jak dodac id utworu do dataframe'u
        return (
            tracks
            .select(
                F.col("track.key"),
                F.col("track.num_samples"),
                F.col("track.tempo"),
                F.col("track.loudness"),
            )
        )