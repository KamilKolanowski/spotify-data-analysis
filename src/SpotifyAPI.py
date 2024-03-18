import requests
import os
from dotenv import load_dotenv


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
