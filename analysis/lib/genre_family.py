"""Normalize genre strings into broad families for cross-story analysis."""

from __future__ import annotations

GENRE_FAMILIES: dict[str, list[str]] = {
    "Folk": [
        "folk", "folk rock", "folk-rock", "acoustic", "singer-songwriter",
        "traditional folk", "contemporary folk", "americana", "roots",
        "celtic", "irish folk", "bluegrass", "old-time",
    ],
    "Rock": [
        "rock", "classic rock", "hard rock", "soft rock", "arena rock",
        "progressive rock", "psychedelic rock", "southern rock",
        "glam rock", "art rock", "alternative rock", "indie rock",
        "garage rock", "blues rock", "roots rock", "heartland rock",
    ],
    "Punk": [
        "punk", "punk rock", "post-punk", "hardcore", "pop punk",
        "anarcho-punk", "garage punk", "oi", "skate punk",
    ],
    "Metal": [
        "metal", "heavy metal", "thrash metal", "death metal",
        "black metal", "doom metal", "power metal", "progressive metal",
        "nu metal", "metalcore", "stoner metal",
    ],
    "Hip-Hop": [
        "hip hop", "hip-hop", "rap", "grime", "trap", "conscious rap",
        "boom bap", "alternative hip hop", "turntablism",
    ],
    "Electronic": [
        "electronic", "electronica", "edm", "techno", "house",
        "ambient", "synth-pop", "synthwave", "drum and bass",
        "dubstep", "trip hop", "downtempo", "idm",
    ],
    "Jazz": [
        "jazz", "vocal jazz", "smooth jazz", "bebop", "free jazz",
        "fusion", "jazz fusion", "bossa nova", "latin jazz",
        "cool jazz", "swing", "big band",
    ],
    "Country": [
        "country", "country rock", "outlaw country", "alt-country",
        "country folk", "honky tonk", "nashville", "western",
        "country pop", "red dirt", "texas country",
    ],
    "Blues": [
        "blues", "blues rock", "electric blues", "delta blues",
        "chicago blues", "rhythm and blues", "r&b", "soul",
        "soul blues", "contemporary blues",
    ],
    "Pop": [
        "pop", "pop rock", "power pop", "dream pop", "indie pop",
        "adult contemporary", "easy listening", "soft pop",
        "baroque pop", "chamber pop",
    ],
    "Classical": [
        "classical", "chamber", "orchestral", "choral",
        "contemporary classical", "string quartet", "opera",
    ],
    "Reggae": [
        "reggae", "ska", "dub", "rocksteady", "dancehall",
        "roots reggae",
    ],
    "World": [
        "world", "afrobeat", "afropop", "latin", "salsa",
        "cumbia", "fado", "flamenco", "klezmer", "enka",
        "qawwali", "highlife", "soukous", "mbalax",
    ],
}

_LOOKUP: dict[str, str] = {}
for family, terms in GENRE_FAMILIES.items():
    for term in terms:
        _LOOKUP[term.lower()] = family


def classify_genre(genre: str | None) -> str:
    """Map a genre string to a broad family. Returns 'Other' if no match."""
    if not genre or not isinstance(genre, str):
        return "Unknown"
    g = genre.strip().lower()
    if g in _LOOKUP:
        return _LOOKUP[g]
    for term, family in _LOOKUP.items():
        if term in g:
            return family
    return "Other"


def classify_genre_series(series):
    """Apply genre classification to a pandas Series."""
    return series.map(classify_genre)
