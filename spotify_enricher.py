"""Spotify integration helpers for Dylan cover enrichment."""

from __future__ import annotations

import base64
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from fuzzy_matcher import FuzzyMatcher, MatchCandidate, MatchQuery, standardize


logger = logging.getLogger(__name__)


@dataclass
class SpotifyConfig:
    """Configuration for interacting with the Spotify Web API."""

    client_id: str
    client_secret: str
    redirect_uri: Optional[str] = None
    market: str = "US"
    rate_limit_sleep: float = 0.25
    min_match_score: float = 0.6
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
        self.matcher = FuzzyMatcher(min_score=config.min_match_score)

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._cache: dict[tuple[str, str], dict[str, object]] = {}

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
        failed_count = 0
        for idx, row in enumerate(covers_df.to_dict(orient="records"), 1):
            row = dict(row)
            if idx % 100 == 0:
                logger.info("Progress: %d/%d (%d failed)", idx, len(covers_df), failed_count)

            title = row.get("recording_title") or row.get("work_title")
            artist_name = row.get("cover_artist_name") or row.get("artist_names")
            album = row.get("release_title")
            if not isinstance(album, str):
                album = None
            year = None
            release_date = row.get("first_release_date")
            if release_date and isinstance(release_date, str) and len(release_date) >= 4:
                try:
                    year = int(release_date[:4])
                except ValueError:
                    pass

            try:
                if title and len(title) > 200:
                    logger.warning("Skipping long title: %s...", title[:100])
                    enriched_rows.append(row)
                    failed_count += 1
                    continue

                spotify_info = self.lookup_track(title, artist_name, album=album, year=year)
                if spotify_info:
                    row.update(spotify_info)
            except (requests.RequestException, ValueError, KeyError) as e:
                logger.warning("Failed to enrich '%s' by '%s': %s", title, artist_name, e)
                failed_count += 1

            enriched_rows.append(row)

        logger.info("Enrichment complete: %d total, %d failed/skipped", len(enriched_rows), failed_count)
        return pd.DataFrame(enriched_rows)

    # ------------------------------------------------------------------
    # Spotify interactions
    # ------------------------------------------------------------------
    def lookup_track(
        self,
        title: Optional[str],
        artist: Optional[str],
        album: Optional[str] = None,
        year: Optional[int] = None,
    ) -> dict[str, object]:
        """Search Spotify and match using cascading fuzzy logic.

        Tries progressively broader queries:
        1. track + artist + album  (most specific Spotify query)
        2. track + artist          (standard query)
        3. track only              (broadest, high title threshold)
        """
        if not title:
            return {}

        cache_key = (standardize(title), standardize(artist))
        if cache_key in self._cache:
            return self._cache[cache_key]

        query = MatchQuery(title=title, artist=artist, album=album, year=year)

        # Try progressively broader Spotify search queries
        search_queries = self._build_search_queries(title, artist, album)

        all_candidates: list[MatchCandidate] = []
        seen_ids: set[str] = set()

        for search_q in search_queries:
            params = {"q": search_q, "type": "track", "limit": 10, "market": self.config.market}
            response = self._request("GET", "https://api.spotify.com/v1/search", params=params)
            data = response.json()
            for item in data.get("tracks", {}).get("items", []):
                track_id = item.get("id")
                if track_id in seen_ids:
                    continue
                seen_ids.add(track_id)
                all_candidates.append(self._item_to_candidate(item))

        if not all_candidates:
            logger.debug("No Spotify results for '%s' by '%s'", title, artist)
            self._cache[cache_key] = {}
            return {}

        result = self.matcher.match(query, all_candidates)
        if not result:
            logger.debug(
                "No match above threshold for '%s' by '%s' (%d candidates)",
                title, artist, len(all_candidates),
            )
            self._cache[cache_key] = {}
            return {}

        best = result.candidate
        match = {
            "spotify_track_id": best.data.get("id"),
            "spotify_track_name": best.title,
            "spotify_artist_name": best.artist,
            "spotify_popularity": best.popularity,
            "spotify_album_name": best.album,
            "spotify_release_date": best.data.get("release_date"),
            "spotify_duration_ms": best.data.get("duration_ms"),
            "spotify_is_explicit": best.data.get("explicit"),
            "spotify_external_url": best.data.get("external_url"),
            "spotify_match_score": result.score,
            "spotify_match_tier": result.tier_name,
        }

        self._cache[cache_key] = match
        return match

    @staticmethod
    def _build_search_queries(title: str, artist: Optional[str], album: Optional[str]) -> list[str]:
        """Build progressively broader Spotify search queries."""
        queries = []
        std_title = standardize(title)
        std_artist = standardize(artist)
        std_album = standardize(album)

        if std_title and std_artist and std_album:
            queries.append(f'track:"{std_title}" artist:"{std_artist}" album:"{std_album}"')
        if std_title and std_artist:
            queries.append(f'track:"{std_title}" artist:"{std_artist}"')
        if std_title:
            queries.append(f'track:"{std_title}"')
        return queries

    @staticmethod
    def _item_to_candidate(item: dict) -> MatchCandidate:
        """Convert a Spotify API track item to a MatchCandidate."""
        artist_names = ", ".join(a.get("name", "") for a in item.get("artists", []))
        album = item.get("album", {})
        album_name = album.get("name", "")
        release_date = album.get("release_date", "")
        year = None
        if release_date and len(release_date) >= 4:
            try:
                year = int(release_date[:4])
            except ValueError:
                pass

        return MatchCandidate(
            title=item.get("name", ""),
            artist=artist_names,
            album=album_name,
            year=year,
            popularity=item.get("popularity", 0),
            data={
                "id": item.get("id"),
                "release_date": release_date,
                "duration_ms": item.get("duration_ms"),
                "explicit": item.get("explicit"),
                "external_url": item.get("external_urls", {}).get("spotify"),
            },
        )

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
        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
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

