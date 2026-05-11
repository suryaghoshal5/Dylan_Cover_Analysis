import os
from pathlib import Path
from spotify_enricher import SpotifyEnricher, SpotifyConfig
from dotenv import load_dotenv

load_dotenv()

config = SpotifyConfig(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    data_dir=Path("data"),
    covers_filename="dylan_covers.csv",
    output_filename="dylan_covers_with_popularity.csv"
)

enricher = SpotifyEnricher(config)
enricher.enrich()
