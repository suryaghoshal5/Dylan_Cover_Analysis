"""Spotify integration helpers for Dylan cover enrichment."""

from __future__ import annotations

import base64
import logging
import os
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests


logger = logging.getLogger(__name__)


@dataclass
class SpotifyConfig:
    """Configuration for interacting with the Spotify Web API."""

    client_id: str
    client_secret: str
    redirect_uri: Optional[str] = None
    market: str = "US"
    rate_limit_sleep: float = 0.25
    data_dir: Path = Path("data")
    covers_filename: str = "dylan_covers.csv"
    output_filename: str = "dylan_covers_with_popularity.csv"


class SpotifyEnricher:
    """Enrich Dylan cover recordings with Spotify metadata."""

    def __init__(self, config: SpotifyConfig) -> None:
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._cache: Dict[Tuple[str, str], Dict[str, object]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def enrich(self, covers_path: Optional[Path] = None, output_path: Optional[Path] = None) -> pd.DataFrame:
        """Load cover data, enrich it with Spotify popularity and export to CSV."""

        covers_path = covers_path or (self.data_dir / self.config.covers_filename)
        output_path = output_path or (self.data_dir / self.config.output_filename)

        if not covers_path.exists():
            raise FileNotFoundError(
                f"Cover dataset {covers_path} does not exist. Run the MusicBrainz parser first."
            )

        covers_df = pd.read_csv(covers_path)
        enriched_df = self.enrich_dataframe(covers_df)
        enriched_df.to_csv(output_path, index=False)
        logger.info("Exported Spotify enriched covers to %s", output_path)
        return enriched_df

    def enrich_dataframe(self, covers_df: pd.DataFrame) -> pd.DataFrame:
        if covers_df.empty:
            return covers_df.copy()

        enriched_rows = []
        for row in covers_df.to_dict(orient="records"):
            title = row.get("recording_title") or row.get("work_title")
            artist_name = row.get("cover_artist_name") or row.get("artist_names")
            spotify_info = self.lookup_track(title, artist_name)
            if spotify_info:
                row.update(spotify_info)
            enriched_rows.append(row)

        return pd.DataFrame(enriched_rows)

    # ------------------------------------------------------------------
    # Spotify interactions
    # ------------------------------------------------------------------
    def lookup_track(self, title: Optional[str], artist: Optional[str]) -> Dict[str, object]:
        if not title:
            return {}

        cache_key = (title.lower(), (artist or "").lower())
        if cache_key in self._cache:
            return self._cache[cache_key]

        query_parts = []
        if title:
            query_parts.append(f'track:"{title}"')
        if artist:
            query_parts.append(f'artist:"{artist}"')
        query = " ".join(query_parts)

        params = {"q": query, "type": "track", "limit": 5, "market": self.config.market}
        response = self._request("GET", "https://api.spotify.com/v1/search", params=params)
        data = response.json()
        items = data.get("tracks", {}).get("items", [])
        if not items:
            logger.debug("Spotify search returned no results for query %s", query)
            self._cache[cache_key] = {}
            return {}

        ranked_items = self._rank_results(title, artist, items)
        best = ranked_items[0]
        match = {
            "spotify_track_id": best.get("id"),
            "spotify_track_name": best.get("name"),
            "spotify_artist_name": ", ".join(artist.get("name") for artist in best.get("artists", [])),
            "spotify_popularity": best.get("popularity"),
            "spotify_album_name": best.get("album", {}).get("name"),
            "spotify_release_date": best.get("album", {}).get("release_date"),
            "spotify_duration_ms": best.get("duration_ms"),
            "spotify_is_explicit": best.get("explicit"),
            "spotify_external_url": best.get("external_urls", {}).get("spotify"),
            "spotify_match_score": best.get("_match_score"),
        }

        self._cache[cache_key] = match
        return match

    def _rank_results(
        self,
        title: str,
        artist: Optional[str],
        items: list[dict],
    ) -> list[dict]:
        results = []
        for item in items:
            track_title = item.get("name", "")
            track_artists = ", ".join(artist.get("name", "") for artist in item.get("artists", []))
            title_score = SequenceMatcher(None, title.lower(), track_title.lower()).ratio()
            if artist:
                artist_score = SequenceMatcher(None, artist.lower(), track_artists.lower()).ratio()
            else:
                artist_score = 0.5
            popularity_score = (item.get("popularity") or 0) / 100.0
            combined = (title_score * 0.5) + (artist_score * 0.3) + (popularity_score * 0.2)
            enriched = dict(item)
            enriched["_match_score"] = round(combined, 4)
            results.append(enriched)

        results.sort(key=lambda entry: entry["_match_score"], reverse=True)
        return results

    # ------------------------------------------------------------------
    # Token management and HTTP helpers
    # ------------------------------------------------------------------
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        self._ensure_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._token}"

        while True:
            response = self.session.request(method, url, headers=headers, timeout=60, **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "1"))
                logger.warning("Spotify rate limit hit. Sleeping for %s seconds", retry_after)
                time.sleep(retry_after + self.config.rate_limit_sleep)
                continue
            response.raise_for_status()
            return response

    def _ensure_token(self) -> None:
        now = time.time()
        if self._token and now < self._token_expiry - 30:
            return

        token_url = "https://accounts.spotify.com/api/token"
        credentials = f"{self.config.client_id}:{self.config.client_secret}".encode()
        encoded = base64.b64encode(credentials).decode()
        headers = {"Authorization": f"Basic {encoded}"}
        data = {"grant_type": "client_credentials"}
        response = self.session.post(token_url, headers=headers, data=data, timeout=30)
        if response.status_code != 200:
            logger.error("Spotify authentication failed: %s", response.text)
            response.raise_for_status()

        payload = response.json()
        self._token = payload["access_token"]
        self._token_expiry = time.time() + payload.get("expires_in", 3600)
        logger.info("Obtained Spotify access token")


def load_config_from_env(data_dir: Path | str = Path("data")) -> SpotifyConfig:
    """Create a :class:`SpotifyConfig` using environment variables."""

    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
    if not client_id or not client_secret:
        raise RuntimeError("Spotify credentials not found in environment variables")
    return SpotifyConfig(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        data_dir=Path(data_dir),
    )


__all__ = ["SpotifyConfig", "SpotifyEnricher", "load_config_from_env"]

