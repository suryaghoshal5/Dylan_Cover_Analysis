"""Extract Bob Dylan works, recordings and cover versions from MusicBrainz.

The project aims to build a comprehensive list of Bob Dylan compositions and
all known recordings that cover those works.  The MusicBrainz database is the
authoritative source for both the works metadata and the recording catalogue.
Depending on the deployment environment we either query a locally imported
MusicBrainz database (preferred) or fall back to the public web service API.

The module exposes the :class:`MusicBrainzParser` class that orchestrates the
data gathering workflow and exports three CSV files:

``dylan_works.csv``
    Metadata about every work associated with Bob Dylan.
``dylan_recordings.csv``
    All recordings of Dylan works, regardless of the performing artist.
``dylan_covers.csv``
    Subset of ``dylan_recordings.csv`` that excludes Bob Dylan as the main
    performer – i.e. cover versions.

The API centric path respects MusicBrainz' one request per second limit by
throttling calls accordingly.  Every exported CSV includes timestamped columns
so analytical pipelines downstream can track update recency.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable
from typing import Optional

import pandas as pd
import requests

try:  # SQLAlchemy is optional for environments without a local database.
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except ImportError:  # pragma: no cover - optional dependency
    Engine = None  # type: ignore
    create_engine = None  # type: ignore


logger = logging.getLogger(__name__)


DEFAULT_USER_AGENT = "Dylan-Cover-Analysis/1.0 (dylan-cover-analysis@example.com)"


@dataclass
class ParserConfig:
    """Configuration options for the :class:`MusicBrainzParser`."""

    artist_name: str = "Bob Dylan"
    data_dir: Path = Path("data")
    user_agent: str = DEFAULT_USER_AGENT
    sleep_seconds: float = 1.1
    use_db_first: bool = True


def _normalise_list(values: Optional[Iterable[str]]) -> str:
    if not values:
        return ""
    return ";".join(sorted({value for value in values if value}))


class MusicBrainzParser:
    """Parse works, recordings and covers attributed to Bob Dylan."""

    def __init__(
        self,
        parser_config: Optional[ParserConfig] = None,
        db_url: Optional[str] = None,
    ) -> None:
        self.config = parser_config or ParserConfig()
        self.data_dir = Path(self.config.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers["User-Agent"] = self.config.user_agent
        self.base_url = "https://musicbrainz.org/ws/2"

        self.db_engine: Optional[Engine] = None
        if db_url:
            if create_engine is None:
                raise RuntimeError(
                    "SQLAlchemy is required for database access but is not installed"
                )
            logger.info("Creating SQLAlchemy engine for %s", db_url)
            self.db_engine = create_engine(db_url)
        elif self.config.use_db_first:
            db_url = os.getenv("MUSICBRAINZ_DB_URL")
            if db_url and create_engine is not None:
                logger.info("Creating SQLAlchemy engine from MUSICBRAINZ_DB_URL")
                self.db_engine = create_engine(db_url)

        self._artist_id: Optional[str] = None
        self._artist_numeric_id: Optional[int] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Execute the full extraction pipeline."""

        artist_id = self.get_artist_id()
        works_df = self.fetch_works(artist_id)
        recordings_df = self.fetch_recordings(works_df)
        covers_df = self.identify_covers(recordings_df)

        works_path = self.data_dir / "dylan_works.csv"
        recordings_path = self.data_dir / "dylan_recordings.csv"
        covers_path = self.data_dir / "dylan_covers.csv"

        works_df.to_csv(works_path, index=False)
        recordings_df.to_csv(recordings_path, index=False)
        covers_df.to_csv(covers_path, index=False)

        logger.info("Exported %s", works_path)
        logger.info("Exported %s", recordings_path)
        logger.info("Exported %s", covers_path)

        return works_df, recordings_df, covers_df

    # ------------------------------------------------------------------
    # Artist discovery
    # ------------------------------------------------------------------
    def get_artist_id(self) -> str:
        """Resolve the MusicBrainz ID for the configured artist."""

        if self._artist_id:
            return self._artist_id

        if self.db_engine is not None:
            try:
                with self.db_engine.connect() as connection:
                    query = text(
                        """
                        SELECT gid, id
                        FROM artist
                        WHERE lower(name) = lower(:name)
                        ORDER BY begin_date_year NULLS FIRST, sort_name
                        LIMIT 1
                        """
                    )
                    result = connection.execute(query, {"name": self.config.artist_name})
                    row = result.mappings().first()
                if row:
                    self._artist_id = row["gid"]
                    self._artist_numeric_id = row["id"]
                    logger.info(
                        "Resolved artist '%s' to %s (numeric id %s) via database",
                        self.config.artist_name,
                        self._artist_id,
                        self._artist_numeric_id,
                    )
                    return self._artist_id
            except (OSError, RuntimeError) as exc:
                logger.warning("Failed to resolve artist via database, falling back to API: %s", exc)

        logger.info(
            "Resolving MusicBrainz artist id for '%s' via web service",
            self.config.artist_name,
        )
        params = {
            "query": f"artist:\"{self.config.artist_name}\"",
            "fmt": "json",
            "limit": 1,
        }
        data = self._get_json("artist", params)
        artists = data.get("artists", [])
        if not artists:
            raise RuntimeError(f"Artist '{self.config.artist_name}' not found on MusicBrainz")

        artist = artists[0]
        self._artist_id = artist["id"]
        # ``gid`` is the UUID value we already assigned to ``self._artist_id``.
        # Numeric identifiers are only available when talking to the local
        # database, therefore we intentionally reset the cached numeric id here
        # to avoid accidentally using stale values from a failed DB lookup.
        self._artist_numeric_id = None
        logger.info("Resolved artist '%s' to %s via API", self.config.artist_name, self._artist_id)
        return self._artist_id

    # ------------------------------------------------------------------
    # Works
    # ------------------------------------------------------------------
    def fetch_works(self, artist_id: str) -> pd.DataFrame:
        """Fetch works composed by Bob Dylan."""

        if self.db_engine is not None:
            if self._artist_numeric_id is None:
                logger.debug(
                    "Skipping database work fetch because numeric artist id is unknown"
                )
            else:
                try:
                    return self._fetch_works_from_db()
                except (OSError, RuntimeError) as exc:
                    logger.warning(
                        "Failed to fetch works from database, falling back to API: %s", exc
                    )

        return self._fetch_works_from_api(artist_id)

    def _fetch_works_from_db(self) -> pd.DataFrame:
        if self.db_engine is None or self._artist_numeric_id is None:
            raise RuntimeError("Database engine is not initialised")

        query = text(
            """
            SELECT
                w.gid AS work_id,
                w.name AS title,
                wt.name AS type,
                (SELECT i.iswc FROM iswc i WHERE i.work = w.id LIMIT 1) AS iswc,
                w.comment AS disambiguation
            FROM work w
            LEFT JOIN work_type wt ON w.type = wt.id
            JOIN l_artist_work law ON law.entity1 = w.id
            JOIN link lnk ON lnk.id = law.link
            JOIN link_type lt ON lt.id = lnk.link_type
            WHERE law.entity0 = :artist_id
            ORDER BY w.name
            """
        )

        with self.db_engine.connect() as connection:
            rows = connection.execute(query, {"artist_id": self._artist_numeric_id})
            works = [dict(row) for row in rows.mappings()]

        df = pd.DataFrame(works)
        for col in ("aliases", "relations", "attributes"):
            if col not in df.columns:
                df[col] = ""
        df["relations"] = "database"
        expected = ["work_id", "title", "type", "language", "iswc", "aliases", "relations", "attributes", "disambiguation"]
        for col in expected:
            if col not in df.columns:
                df[col] = None
        df = df[expected]
        logger.info("Fetched %d works from database", len(df))
        return df

    def _fetch_works_from_api(self, artist_id: str) -> pd.DataFrame:
        logger.info("Fetching works for artist %s via MusicBrainz API", artist_id)
        works: list[dict[str, object]] = []
        offset = 0
        limit = 100

        while True:
            params = {
                "artist": artist_id,
                "limit": limit,
                "offset": offset,
                "fmt": "json",
                "inc": "aliases+artist-rels+iswcs+tags",
            }
            data = self._get_json("work", params)
            for work in data.get("works", []):
                works.append(
                    {
                        "work_id": work.get("id"),
                        "title": work.get("title"),
                        "type": work.get("type"),
                        "language": work.get("language"),
                        "iswc": _normalise_list(work.get("iswcs")),
                        "aliases": _normalise_list(
                            alias.get("name") for alias in work.get("aliases", [])
                        ),
                        "relations": json.dumps(work.get("relations", [])),
                        "attributes": json.dumps(work.get("attributes", [])),
                        "disambiguation": work.get("disambiguation"),
                    }
                )

            if offset + limit >= data.get("count", 0):
                break
            offset += limit

        df = pd.DataFrame(works)
        logger.info("Fetched %d works via API", len(df))
        return df

    # ------------------------------------------------------------------
    # Recordings
    # ------------------------------------------------------------------
    def fetch_recordings(self, works_df: pd.DataFrame) -> pd.DataFrame:
        if works_df.empty:
            return pd.DataFrame()

        checkpoint_path = self.data_dir / "dylan_recordings_checkpoint.csv"
        recordings: list[dict[str, object]] = []
        completed_works: set[str] = set()

        if checkpoint_path.exists():
            checkpoint_df = pd.read_csv(checkpoint_path)
            recordings = checkpoint_df.to_dict(orient="records")
            completed_works = set(checkpoint_df["work_id"].unique())
            logger.info(
                "Resumed from checkpoint: %d recordings for %d works",
                len(recordings), len(completed_works),
            )

        total = len(works_df)
        for idx, (_, work) in enumerate(works_df.iterrows(), 1):
            work_id = work["work_id"]
            if work_id in completed_works:
                continue
            recordings.extend(self._fetch_recordings_for_work(work_id, work))

            if idx % 50 == 0:
                pd.DataFrame(recordings).to_csv(checkpoint_path, index=False)
                logger.info("Checkpoint saved: %d/%d works, %d recordings", idx, total, len(recordings))

        df = pd.DataFrame(recordings)
        pre_dedup = len(df)
        if not df.empty:
            df = df.drop_duplicates(subset="recording_id", keep="first")
        logger.info("Fetched %d recordings (%d before dedup)", len(df), pre_dedup)

        if checkpoint_path.exists():
            checkpoint_path.unlink()

        return df

    def _fetch_recordings_for_work(self, work_id: str, work_row: pd.Series) -> list[dict[str, object]]:
        logger.debug("Fetching recordings for work %s", work_id)
        limit = 100
        offset = 0
        results: list[dict[str, object]] = []

        while True:
            params = {
                "work": work_id,
                "fmt": "json",
                "limit": limit,
                "offset": offset,
                "inc": "artist-credits+releases+isrcs",
            }
            data = self._get_json("recording", params)
            for recording in data.get("recordings", []):
                artist_credit = recording.get("artist-credit", [])
                artist_names = [credit.get("name") for credit in artist_credit if credit.get("name")]
                artist_ids = [credit.get("artist", {}).get("id") for credit in artist_credit]

                # Pick the earliest release to avoid per-release row duplication
                release = self._pick_earliest_release(recording.get("releases") or [])

                results.append(
                    {
                        "work_id": work_id,
                        "work_title": work_row.get("title"),
                        "recording_id": recording.get("id"),
                        "recording_title": recording.get("title"),
                        "recording_length_ms": recording.get("length"),
                        "artist_names": _normalise_list(artist_names),
                        "artist_ids": _normalise_list(artist_ids),
                        "is_bob_dylan": self._is_bob_dylan_recording(artist_credit),
                        "release_id": release.get("id"),
                        "release_title": release.get("title"),
                        "first_release_date": release.get("date") or recording.get("first-release-date"),
                        "isrcs": _normalise_list(recording.get("isrcs")),
                    }
                )

            if offset + limit >= data.get("count", 0):
                break
            offset += limit

        if results:
            logger.debug("Retrieved %d recordings for work %s", len(results), work_id)
        return results

    @staticmethod
    def _pick_earliest_release(releases: list[dict]) -> dict:
        """Return the release with the earliest date, or the first one."""
        if not releases:
            return {}
        dated = [(r, r.get("date", "") or "") for r in releases]
        dated.sort(key=lambda pair: pair[1] if pair[1] else "9999")
        return dated[0][0]

    def _is_bob_dylan_recording(self, artist_credit: list[dict[str, object]]) -> bool:
        target_id = self._artist_id
        for credit in artist_credit:
            artist = credit.get("artist")
            if artist and artist.get("id") == target_id:
                return True
        return False

    # ------------------------------------------------------------------
    # Covers
    # ------------------------------------------------------------------
    def identify_covers(self, recordings_df: pd.DataFrame) -> pd.DataFrame:
        if recordings_df.empty:
            return recordings_df

        mask = ~recordings_df["is_bob_dylan"].fillna(False)
        covers = recordings_df.loc[mask].copy()
        covers["cover_artist_name"] = covers["artist_names"]
        covers["cover_artist_ids"] = covers["artist_ids"]
        logger.info("Identified %d cover recordings", len(covers))
        return covers

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------
    def _get_json(self, endpoint: str, params: dict[str, object], _max_retries: int = 5) -> dict[str, object]:
        """Perform a GET request against the MusicBrainz web service with exponential backoff."""

        url = f"{self.base_url}/{endpoint}"
        for attempt in range(_max_retries):
            response = self.session.get(url, params=params, timeout=60)
            if response.status_code == 503:
                wait_time = int(response.headers.get("Retry-After", 2 ** attempt)) + 1
                logger.warning(
                    "Rate limited by MusicBrainz (attempt %d/%d), sleeping %ds",
                    attempt + 1, _max_retries, wait_time,
                )
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            time.sleep(self.config.sleep_seconds)
            return response.json()

        response.raise_for_status()
        return response.json()


__all__ = ["ParserConfig", "MusicBrainzParser"]

