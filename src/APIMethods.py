from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
import json
from SpotifyAPI import SpotifyAPI

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


sp = SpotifyAPI()


class APIMethods:

    @staticmethod
    def process_single_row_objects(endpoint: str, objects: list):
        objects_to_process = []

        for obj in objects:
            objects_to_process.append(sp.parse_json(endpoint, obj))

        obj_dict = sc.parallelize(objects_to_process).map(lambda x: json.dumps(x))
        objs = spark.read.json(obj_dict)

        return objs


    def read_json_to_df(self, obj):
        return spark.read.json(sc.parallelize([json.dumps(obj)]))

    # Definition for transformation of playlists
    def process_playlists(self, playlist_id: str):
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

    def process_audio_analysis(self, tracks_ids: list):
        # tracks = self.process_single_row_objects("audio-analysis", tracks_ids)
        rows = []
        for track_id in tracks_ids:
            audio_analysis_data = self.process_single_row_objects("audio-analysis", tracks_ids)
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
