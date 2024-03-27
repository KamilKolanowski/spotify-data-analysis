from pyspark.sql import SparkSession
from dotenv import load_dotenv
import requests
import json
import os


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


class SpotifyAPI:
    load_dotenv()

    def __init__(self) -> object:
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.base_url = os.getenv("BASE_URL")
        self.token_url = os.getenv("TOKEN_URL")

    def get_access_token(self):
        response = requests.post(
            self.token_url,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
        )

        return response.json()["access_token"]

    def parse_json(self, endpoint, object_id):
        response = requests.get(f"{self.base_url}/{endpoint}/{object_id}",
                                headers=dict(Authorization=f"Bearer {self.get_access_token()}"))

        return response.json()

    def process_single_row_objects(self, endpoint: str, objects: list):
        objects_to_process = []

        for obj in objects:
            objects_to_process.append(self.parse_json(endpoint, obj))

        obj_dict = sc.parallelize(objects_to_process).map(lambda x: json.dumps(x))
        objs = spark.read.json(obj_dict)

        return objs

    @staticmethod
    def read_json_to_df(obj):
        return spark.read.json(sc.parallelize([json.dumps(obj)]))
