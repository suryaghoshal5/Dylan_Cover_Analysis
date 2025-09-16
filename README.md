# 🎸 Dylan Cover Song Analysis Project

## 🎯 Objective

Build a full pipeline to analyze Bob Dylan’s songs and their cover versions across time and platforms. The goal is to:
- Identify all Dylan originals using MusicBrainz
- Collect all cover versions with artist metadata
- Enrich songs with Spotify popularity data
- Build a knowledge graph connecting originals to covers
- Explore cultural influence and musical networks

---

## 🧩 Modules Overview

### 1. `musicbrainz_downloader.py`
Handles fetching and importing the official MusicBrainz database dumps. It can
provision PostgreSQL via Docker, verify checksums, and stream SQL imports.

### 2. `musicbrainz_parser.py`
Extracts Bob Dylan's works, all related recordings, and identifies cover
versions via the MusicBrainz API (or local database when available). Exports:
`data/dylan_works.csv`, `data/dylan_recordings.csv`, `data/dylan_covers.csv`.

### 3. `spotify_enricher.py`
Looks up each cover on Spotify using the Web API and appends track popularity,
album and release metadata. Output: `data/dylan_covers_with_popularity.csv`.

### 4. `main.py`
Command line entry point that orchestrates the full workflow. Flags control
database refresh, MusicBrainz parsing and Spotify enrichment runs.

---

## 📂 Input/Output Files

| Filename                                   | Description |
|--------------------------------------------|-------------|
| `data/dylan_works.csv`                     | All original Dylan works |
| `data/dylan_recordings.csv`                | All recordings linked to Dylan's works |
| `data/dylan_covers.csv`                    | Dylan covers (no Dylan credited) |
| `data/dylan_covers_with_popularity.csv`    | Covers enriched with Spotify data |

---

## 📦 Dependencies

```
pandas
requests
sqlalchemy  # optional for local database access
python-dotenv  # optional for loading credentials
```

---

## 🚦 Usage Instructions

1. `python main.py --refresh-db` (optional) to download/import MusicBrainz.
2. `python main.py --get-covers` to create the MusicBrainz CSV exports.
3. `python main.py --enrich-spotify` after setting Spotify credentials.

---

## 📌 Notes

- No Wikipedia scraping used.
- MusicBrainz: 1 req/sec rate limit
- Requires valid Spotify API credentials in `.env`
- Caching used to reduce redundant API calls

---

## 🔮 Future Extensions

- Add audio features (tempo, valence, key, etc.)
- Compare Spotify vs. live popularity
- Cluster cover artists by decade/genre
