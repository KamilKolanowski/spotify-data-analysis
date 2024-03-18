from APIMethods import APIMethods
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Define new object with API methods to process Spotify data

apm = APIMethods()


if __name__ == "__main__":
    # Prepare playlist dataframe
    playlist_id = "37i9dQZEVXbMDoHDwVN2tF"
    playlists_df = apm.process_playlists(playlist_id)

    # Prepare artists dataframe, based on the list of artists within earlier processed playlist
    artists = playlists_df.select(F.col("artist_id")).distinct().collect()
    artists = [item for artist in artists for item in list(artist)]

    df = apm.process_audio_analysis(["5qaEfEh1AtSdrdrByCP7qR", "1SShxVVBeZBCY7WddnksPz"])
    df.show()

