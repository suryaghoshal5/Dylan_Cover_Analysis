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

### 1. `get_dylan_songs.py`
Fetches all original Bob Dylan works from MusicBrainz.
- Fields captured: `work_id`, `title`, `type`, `language`, `aliases`, `relations`, `attributes`
- Saves data to: `data/works.csv`

### 2. `get_covers.py`
Fetches all known recordings and releases linked to each Dylan work.
- Fields: `recording_id`, `title`, `artist_name`, `release_title`, `release_id`, `first-release-date`
- Saves data to: `data/recordings.csv`

### 3. `get_spotify_data.py`
Enriches each cover with Spotify metadata and popularity.
- Adds: `spotify_track_id`, `popularity`, `album_name`, `release_date`, `duration_ms`, `explicit`
- Saves to: `data/recordings_spotify_enriched.csv`

### 4. `build_graph_db.py`
Builds a song → artist network graph using `pyvis`.
- Nodes: Songs and Artists
- Edges: Covers with popularity weights
- Output: `output/dylan_cover_graph.html`

### 5. `get_live_counts.py` (Optional)
Adds live performance frequency from Setlist.fm or cached data.

---

## 📂 Input/Output Files

| Filename                             | Description |
|--------------------------------------|-------------|
| `data/works.csv`                     | All original Dylan works |
| `data/recordings.csv`                | All recordings linked to Dylan's works |
| `data/recordings_spotify_enriched.csv` | Covers + Spotify popularity |
| `output/dylan_cover_graph.html`     | Interactive visualization of influence |

---

## 📦 Dependencies

```
pandas
requests
spotipy
tqdm
python-dotenv
networkx
pyvis
```

---

## 🚦 Usage Instructions

1. Run `fetch_dylan_works()` in `get_dylan_songs.py`
2. Run `fetch_all_recordings(works_df)` in `get_covers.py`
3. Run `enrich_with_spotify_popularity()` in `get_spotify_data.py`
4. Optional: Run `fetch_live_counts()` in `get_live_counts.py`
5. Generate graph with `build_pyvis_html()` in `build_graph_db.py`

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
