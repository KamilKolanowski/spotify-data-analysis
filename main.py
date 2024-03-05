import requests
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pprint as pp

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


class SpotifyAPI:
    load_dotenv()

    def __init__(self):
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.base_url = "https://api.spotify.com/v1"
        self.token_url = "https://accounts.spotify.com/api/token"


    def get_access_token(self):
        response = requests.post(
            self.token_url,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
        )

        return response.json()["access_token"]

    def parse_json(self, endpoint, id):
        response = requests.get(f"{self.base_url}/{endpoint}/{id}",
                                headers=dict(Authorization=f"Bearer {self.get_access_token()}"))

        return response.json()

    def parse_artists(self, artist_id):
        input_art = self.parse_json("artists", artist_id)

        return [{
            "id": input_art["id"],
            "name": input_art["name"],
            "popularity": input_art["popularity"],
            "genres": input_art["genres"],
        }]

    def parse_playlist_items(self, playlist_id):
        input_playlists = self.parse_json("playlists", f"{playlist_id}/tracks")["items"]

        res_playlist = {}

        for playlist in input_playlists:
            res_playlist.update(playlist["track"])

        return res_playlist["name"]
        # return [{
        #     # "added_at": input_playlist["added_at"]
        #     "track_name": res_playlist["name"],
        #     "artists": res_playlist["artists"],
        #     "track_id": res_playlist["id"],
        #     "duration_ms": res_playlist["duration_ms"]
        #     # "track": playlists["track"]
        # }]
        # return res_playlist
        # return playlists


sp = SpotifyAPI()

###
artists_list = ["6I5JdHLVup9pIjn9g5K20N", "0yKPpbp3T6JTB9ApDMv9SZ", "6c9QzUS4FsfkV31t39lnbU"]
artists = []

for artist in artists_list:
    artists.append(sp.parse_artists(artist))

arts_dict = sc.parallelize(artists).map(lambda x: json.dumps(x))
arts = spark.read.json(arts_dict)


playlists = spark.read.json(sc.parallelize(sp.parse_playlist_items("2ECDxRcG2e9SY8rfQrUfrV")))
playlists.show(100, False)
# arts = (
#     arts
#     .select(
#         F.explode("genres").alias("genre"),
#         F.col("id"),
#         F.col("name"),
#         F.col("popularity")
#     )
# )
#
# arts.show(4, False)

