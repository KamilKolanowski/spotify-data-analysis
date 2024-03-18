from APIMethods import APIMethods
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Define new object with API methods to process Spotify data

apm = APIMethods()

if __name__ == "__main__":
    # Prepare playlist dataframe
    playlist_id = "4IAGMLxu1OLYgj27083hvY"

    top_2023_tracks = [row['track_id'] for row in apm.process_playlists(playlist_id)
                                                     .distinct()
                                                     .select('track_id').collect()]

    audio_analysis_top_2023 = apm.process_audio_analysis(top_2023_tracks)
    audio_analysis_top_2023.show()
