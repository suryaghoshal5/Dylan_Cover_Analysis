"""Reusable cascading fuzzy matcher for music track matching.

Provides standardized string comparison with a tiered fallback strategy
designed for matching MusicBrainz recordings to Spotify tracks (or any
source-to-target track matching scenario).

Match tiers (strictest first, relaxes one criterion at a time):
    1. song + artist + album + year  (all four)
    2. song + artist + album         (drop year)
    3. song + artist                 (drop album)
    4. song only                     (title-only, high threshold)

Each tier scores candidates using weighted field similarity. A match is
accepted at the first tier where a candidate exceeds the confidence
threshold. This avoids false positives from broad queries while still
catching legitimate matches that lack album or year metadata.

Usage::

    from fuzzy_matcher import FuzzyMatcher, MatchCandidate, MatchQuery

    matcher = FuzzyMatcher(min_score=0.6)
    query = MatchQuery(title="Blowin' in the Wind", artist="Stevie Wonder")
    candidates = [
        MatchCandidate(title="Blowin' In The Wind", artist="Stevie Wonder",
                       album="Talking Book", year=1972, data={"spotify_id": "..."}),
    ]
    result = matcher.match(query, candidates)
    if result:
        print(result.score, result.tier, result.candidate.data)
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Optional


def standardize(text: Optional[str]) -> str:
    """Normalize a string for fuzzy comparison.

    Steps:
        1. Unicode NFKD normalization (decompose accented chars)
        2. Strip combining marks (é → e)
        3. Lowercase
        4. Remove parenthetical suffixes: (Live), (Remastered), (Deluxe), etc.
        5. Remove bracketed suffixes: [Bonus Track], etc.
        6. Strip non-alphanumeric characters (keep spaces)
        7. Collapse multiple spaces
        8. Strip leading/trailing whitespace
    """
    if not text or not isinstance(text, str):
        return ""

    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))

    s = s.lower()

    s = re.sub(r"\s*\([^)]*(?:live|remix|remaster|deluxe|mono|stereo|demo|edit|version|bonus|acoustic|alternate|alt\b|single|extended|instrumental|feat\.?|ft\.?)[^)]*\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*\[[^\]]*\]", "", s)

    s = re.sub(r"[^\w\s]", " ", s)

    s = re.sub(r"\s+", " ", s).strip()

    return s


def similarity(a: str, b: str) -> float:
    """Compute similarity ratio between two standardized strings."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


@dataclass
class MatchQuery:
    """What we're trying to match (the source record)."""
    title: str
    artist: Optional[str] = None
    album: Optional[str] = None
    year: Optional[int] = None

    def __post_init__(self):
        self._std_title = standardize(self.title)
        self._std_artist = standardize(self.artist)
        self._std_album = standardize(self.album)


@dataclass
class MatchCandidate:
    """A potential match (from Spotify or another target source)."""
    title: str
    artist: Optional[str] = None
    album: Optional[str] = None
    year: Optional[int] = None
    popularity: float = 0.0
    data: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self._std_title = standardize(self.title)
        self._std_artist = standardize(self.artist)
        self._std_album = standardize(self.album)


@dataclass
class MatchResult:
    """Result of a successful match."""
    candidate: MatchCandidate
    score: float
    tier: int
    tier_name: str
    component_scores: dict[str, float]


# ── Tier definitions ────────────────────────────────────────────────

TIERS = [
    {
        "name": "song+artist+album+year",
        "tier": 1,
        "weights": {"title": 0.35, "artist": 0.30, "album": 0.20, "year": 0.15},
        "required": ["title", "artist", "album", "year"],
    },
    {
        "name": "song+artist+album",
        "tier": 2,
        "weights": {"title": 0.40, "artist": 0.35, "album": 0.25},
        "required": ["title", "artist", "album"],
    },
    {
        "name": "song+artist",
        "tier": 3,
        "weights": {"title": 0.50, "artist": 0.35, "popularity": 0.15},
        "required": ["title", "artist"],
    },
    {
        "name": "song-only",
        "tier": 4,
        "weights": {"title": 0.70, "popularity": 0.30},
        "required": ["title"],
        "min_title_sim": 0.85,
    },
]


class FuzzyMatcher:
    """Cascading fuzzy matcher with tiered fallback.

    Parameters
    ----------
    min_score:
        Minimum combined score to accept a match. Default 0.6.
    year_tolerance:
        How many years apart two releases can be and still score 1.0
        on the year component. Default 1.
    """

    def __init__(self, min_score: float = 0.6, year_tolerance: int = 1) -> None:
        self.min_score = min_score
        self.year_tolerance = year_tolerance

    def match(
        self,
        query: MatchQuery,
        candidates: list[MatchCandidate],
    ) -> Optional[MatchResult]:
        """Try each tier in order, return first match above threshold."""
        if not candidates:
            return None

        for tier_def in TIERS:
            if not self._query_has_fields(query, tier_def["required"]):
                continue

            result = self._try_tier(query, candidates, tier_def)
            if result:
                return result

        return None

    def score_candidate(
        self,
        query: MatchQuery,
        candidate: MatchCandidate,
        weights: dict[str, float],
    ) -> tuple[float, dict[str, float]]:
        """Score a single candidate against a query using given weights."""
        components: dict[str, float] = {}

        if "title" in weights:
            components["title"] = similarity(query._std_title, candidate._std_title)

        if "artist" in weights:
            components["artist"] = similarity(query._std_artist, candidate._std_artist)

        if "album" in weights:
            components["album"] = similarity(query._std_album, candidate._std_album)

        if "year" in weights:
            components["year"] = self._year_similarity(query.year, candidate.year)

        if "popularity" in weights:
            components["popularity"] = candidate.popularity / 100.0 if candidate.popularity else 0.0

        combined = sum(
            components.get(field, 0.0) * weight
            for field, weight in weights.items()
        )

        return combined, components

    # ── Internal ────────────────────────────────────────────────────

    def _try_tier(
        self,
        query: MatchQuery,
        candidates: list[MatchCandidate],
        tier_def: dict,
    ) -> Optional[MatchResult]:
        best_score = 0.0
        best_result: Optional[MatchResult] = None

        for candidate in candidates:
            score, components = self.score_candidate(
                query, candidate, tier_def["weights"]
            )

            min_title = tier_def.get("min_title_sim", 0.0)
            if min_title and components.get("title", 0) < min_title:
                continue

            if score > best_score and score >= self.min_score:
                best_score = score
                best_result = MatchResult(
                    candidate=candidate,
                    score=round(score, 4),
                    tier=tier_def["tier"],
                    tier_name=tier_def["name"],
                    component_scores={k: round(v, 4) for k, v in components.items()},
                )

        return best_result

    @staticmethod
    def _query_has_fields(query: MatchQuery, required: list[str]) -> bool:
        for f in required:
            if f == "title" and not query._std_title:
                return False
            if f == "artist" and not query._std_artist:
                return False
            if f == "album" and not query._std_album:
                return False
            if f == "year" and query.year is None:
                return False
        return True

    def _year_similarity(self, a: Optional[int], b: Optional[int]) -> float:
        if a is None or b is None:
            return 0.5
        diff = abs(a - b)
        if diff <= self.year_tolerance:
            return 1.0
        if diff <= 5:
            return 0.7
        if diff <= 10:
            return 0.4
        return 0.1


__all__ = [
    "FuzzyMatcher",
    "MatchCandidate",
    "MatchQuery",
    "MatchResult",
    "standardize",
    "similarity",
]
