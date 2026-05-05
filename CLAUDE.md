# Dylan Cover Song Analysis - AI Assistant Guide

## 🎯 Project Overview

This project builds a comprehensive data pipeline to analyze Bob Dylan's original compositions and all their cover versions across music history. The goal is to create a knowledge graph connecting original works to cover recordings, enriched with popularity metrics from Spotify.

### Core Objectives
- Identify all Bob Dylan original works using MusicBrainz
- Collect ALL cover versions (no artificial limits)
- Enrich with Spotify popularity and metadata
- Build queryable datasets for cultural influence analysis

### Critical Previous Issue ⚠️
**The project previously FAILED because it capped cover recordings at 100 per song.** This caused the most frequently covered Dylan songs (like "Blowin' in the Wind", "Like a Rolling Stone") to be excluded from the final dataset. **NEVER apply arbitrary caps on cover counts.** The pipeline must collect ALL available covers regardless of volume.

---

## 🏗️ Architecture

The project follows a **three-stage ETL pipeline**:

```
┌─────────────────────────┐
│ 1. MusicBrainz Download │  ← Optional: Local DB setup
└───────────┬─────────────┘
            ↓
┌─────────────────────────┐
│ 2. MusicBrainz Parser   │  ← Extract works, recordings, covers
└───────────┬─────────────┘
            ↓
┌─────────────────────────┐
│ 3. Spotify Enricher     │  ← Add popularity metrics
└─────────────────────────┘
```

### Stage 1: MusicBrainz Database (Optional)
- Downloads official MusicBrainz database dumps (~40GB compressed)
- Can provision PostgreSQL via Docker or use existing instance
- Verifies checksums to ensure data integrity
- Imports SQL dumps into PostgreSQL
- **Benefit**: Faster queries, no rate limits, complete data access

### Stage 2: MusicBrainz Parser (Core)
- Resolves artist ID for Bob Dylan
- Fetches ALL works (compositions) attributed to Dylan
- For each work, fetches ALL recordings (no limits!)
- Identifies covers (recordings where Dylan is not the performer)
- Falls back to MusicBrainz API if local DB unavailable
- **Respects 1 req/sec rate limit** when using API

### Stage 3: Spotify Enricher
- Looks up each cover on Spotify using fuzzy matching
- Retrieves: popularity score, album, release date, duration
- Uses client credentials OAuth flow
- Implements retry logic for rate limits
- Caches results to avoid redundant API calls

---

## 📁 Codebase Structure

```
Dylan_Cover_Analysis/
├── main.py                      # CLI orchestration
├── musicbrainz_downloader.py    # DB dump management
├── musicbrainz_parser.py        # Works/recordings extraction
├── spotify_enricher.py          # Spotify metadata enrichment
├── requirements.txt             # Python dependencies
├── README.md                    # User documentation
├── CLAUDE.md                    # This file - AI guide
└── data/                        # Output directory (git-ignored)
    ├── dylan_works.csv          # All Dylan compositions
    ├── dylan_recordings.csv     # All recordings of Dylan works
    ├── dylan_covers.csv         # Cover versions only
    └── dylan_covers_with_popularity.csv  # Spotify-enriched covers
```

### Module Responsibilities

#### `main.py`
- **Purpose**: Command-line entry point
- **Key functions**: 
  - `build_argument_parser()`: Defines all CLI flags
  - `main()`: Orchestrates the pipeline stages
- **Dependencies**: Imports all three modules
- **Optional dependency**: `python-dotenv` for `.env` file support

#### `musicbrainz_downloader.py`
- **Purpose**: Download and import MusicBrainz database dumps
- **Key classes**:
  - `PostgresConfig`: DB connection parameters
  - `DumpConfig`: Which dumps to download
  - `MusicBrainzDownloader`: Main orchestration class
- **Key methods**:
  - `resolve_release()`: Gets latest dump version
  - `download_dump()`: Downloads with checksum verification
  - `extract_dump()`: Unpacks `.tar.bz2` archives
  - `import_sql_files()`: Streams SQL into PostgreSQL
  - `ensure_postgres_database()`: Provisions DB (Docker or local)
- **Dependencies**: `requests` for downloads, `subprocess` for Docker/psql
- **User-Agent**: `Dylan-Cover-Analysis/1.0` (MusicBrainz requires identification)

#### `musicbrainz_parser.py`
- **Purpose**: Extract Dylan works and cover recordings
- **Key classes**:
  - `ParserConfig`: Artist name, data directory, rate limits
  - `MusicBrainzParser`: Main extraction logic
- **Key methods**:
  - `run()`: Full pipeline - returns 3 DataFrames
  - `get_artist_id()`: Resolve Dylan's MusicBrainz UUID
  - `fetch_works()`: Get all Dylan compositions (DB or API)
  - `fetch_recordings()`: **Get ALL recordings per work (NO CAPS!)**
  - `identify_covers()`: Filter out Dylan's own recordings
- **Critical**: Uses pagination (`offset`/`limit`) to fetch ALL results
- **Rate limiting**: Sleeps 1.1 seconds between API calls
- **Fallback logic**: Tries DB first, then API if DB unavailable

#### `spotify_enricher.py`
- **Purpose**: Add Spotify popularity and metadata
- **Key classes**:
  - `SpotifyConfig`: API credentials and settings
  - `SpotifyEnricher`: Lookup and matching logic
- **Key methods**:
  - `enrich()`: Load covers CSV, enrich, export
  - `lookup_track()`: Search Spotify for track
  - `_rank_results()`: Score matches using fuzzy string matching
  - `_ensure_token()`: OAuth client credentials flow
- **Matching algorithm**: 
  - Title similarity: 50% weight
  - Artist similarity: 30% weight
  - Popularity: 20% weight
- **Caching**: In-memory cache to avoid duplicate lookups

---

## 🔄 Development Workflow

### First-Time Setup
```bash
# 1. Clone and enter repo
git clone <repo-url>
cd Dylan_Cover_Analysis

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file for Spotify credentials
cat > .env << EOF
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:8888/callback
EOF

# 5. (Optional) Set up local MusicBrainz database
python main.py --refresh-db --use-docker
```

### Running the Pipeline

#### Quick API-Based Run (No DB)
```bash
python main.py --get-covers --enrich-spotify
```
- Uses MusicBrainz API (1 req/sec limit)
- Takes several hours for all Dylan works
- Suitable for testing or small datasets

#### Full Local DB Run (Recommended)
```bash
# Step 1: Download and import MusicBrainz (one-time, ~6-12 hours)
python main.py --refresh-db --use-docker

# Step 2: Extract covers from local DB (fast!)
python main.py --get-covers --db-url "postgresql://musicbrainz:musicbrainz@localhost:5432/musicbrainz"

# Step 3: Enrich with Spotify
python main.py --enrich-spotify
```

#### Individual Stages
```bash
# Only download/refresh database
python main.py --refresh-db

# Only extract covers (requires prior DB or uses API)
python main.py --get-covers

# Only enrich with Spotify (requires dylan_covers.csv)
python main.py --enrich-spotify
```

### Common Development Tasks

#### Adding a New Artist
```bash
python main.py --artist "Leonard Cohen" --get-covers
```

#### Using Custom Data Directory
```bash
python main.py --data-dir ./output --get-covers --enrich-spotify
```

#### Debugging/Verbose Logging
```bash
python main.py --log-level DEBUG --get-covers
```

#### Skip Checksum Verification (Faster Development)
```bash
python main.py --refresh-db --skip-verify
```

---

## 🗂️ Data Pipeline Details

### CSV Schemas

#### `dylan_works.csv`
Columns:
- `work_id`: MusicBrainz work UUID
- `title`: Song title
- `type`: Work type (e.g., "Song")
- `language`: Language code
- `iswc`: International Standard Work Code
- `aliases`: Semicolon-separated alternative titles
- `relations`: JSON array of relationships
- `attributes`: JSON array of attributes
- `disambiguation`: Disambiguation text

#### `dylan_recordings.csv`
Columns:
- `work_id`: Links to dylan_works.csv
- `work_title`: Song title
- `recording_id`: MusicBrainz recording UUID
- `recording_title`: Recording title (may differ from work)
- `recording_length_ms`: Duration in milliseconds
- `artist_names`: Semicolon-separated artist names
- `artist_ids`: Semicolon-separated MusicBrainz artist UUIDs
- `is_bob_dylan`: Boolean - True if Dylan performed it
- `release_id`: MusicBrainz release UUID
- `release_title`: Album/release name
- `first_release_date`: YYYY-MM-DD format
- `isrcs`: Semicolon-separated ISRCs

#### `dylan_covers.csv`
Same schema as `dylan_recordings.csv` but filtered to `is_bob_dylan == False`
Additional columns:
- `cover_artist_name`: Copy of `artist_names`
- `cover_artist_ids`: Copy of `artist_ids`

#### `dylan_covers_with_popularity.csv`
All columns from `dylan_covers.csv` plus:
- `spotify_track_id`: Spotify track URI
- `spotify_track_name`: Track name on Spotify
- `spotify_artist_name`: Artist name on Spotify
- `spotify_popularity`: 0-100 popularity score
- `spotify_album_name`: Album name
- `spotify_release_date`: Release date
- `spotify_duration_ms`: Duration in milliseconds
- `spotify_is_explicit`: Boolean
- `spotify_external_url`: Spotify web player URL
- `spotify_match_score`: 0-1 fuzzy matching confidence

### Data Volume Expectations
- **Dylan works**: ~650 compositions
- **Dylan recordings**: ~40,000-60,000 total recordings
- **Dylan covers**: ~35,000-50,000 (most recordings are covers)
- **Spotify matches**: ~70-85% match rate (some covers predate Spotify or aren't available)

---

## ⚙️ Key Conventions

### Code Style
- **Type hints**: Used throughout with `from __future__ import annotations`
- **Dataclasses**: For configuration objects (`@dataclass` decorator)
- **Logging**: All modules use `logging.getLogger(__name__)`
- **Docstrings**: Google-style docstrings for modules and classes
- **Error handling**: Explicit `raise RuntimeError` with descriptive messages
- **Path objects**: Use `pathlib.Path` not raw strings

### Naming Conventions
- **Functions**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private methods**: Prefix with `_` (e.g., `_fetch_works_from_api`)
- **Config classes**: End with `Config` (e.g., `ParserConfig`)

### API Etiquette
- **MusicBrainz**: 
  - MUST include User-Agent header
  - MUST respect 1 request per second limit
  - SHOULD use local DB for production workloads
  - Handle 503 + Retry-After headers
- **Spotify**:
  - Handle 429 rate limits with exponential backoff
  - Cache results to minimize redundant calls
  - Use client credentials (no user auth needed)

### Data Handling
- **CSV exports**: Always use `index=False` when calling `to_csv()`
- **List normalization**: Semicolon-separated, sorted, deduplicated
- **Boolean columns**: Use pandas boolean type (not strings)
- **Date formats**: ISO 8601 (YYYY-MM-DD)
- **Missing data**: Use pandas `NaN`, not empty strings

### Testing Approach
- **Manual testing**: Run pipeline on smaller artists first
- **Data validation**: Check row counts, null percentages
- **Spot checking**: Manually verify a sample of covers on Spotify
- **Idempotency**: Running pipeline twice should produce same results

---

## 🚨 Critical Issues to Avoid

### 1. **NEVER Cap Cover Counts** ⚠️⚠️⚠️
**Historical problem**: Previous iteration limited covers to 100 per song
**Impact**: Most popular Dylan songs were excluded from analysis
**Solution**: Always use pagination to fetch ALL results:
```python
while True:
    data = fetch_page(offset=offset, limit=100)
    results.extend(data["items"])
    if offset + limit >= data["count"]:
        break
    offset += limit
```

### 2. Rate Limit Violations
**Problem**: MusicBrainz will ban your IP if you exceed 1 req/sec
**Solution**: 
- Use `time.sleep(1.1)` after each API call
- Implement local database for production
- Handle 503 responses with `Retry-After` headers

### 3. Incomplete Data Downloads
**Problem**: Network failures during multi-GB downloads
**Solution**:
- Use checksum verification (`--skip-verify` only for testing)
- Implement resume logic (check if files exist before re-downloading)
- Use streaming downloads with chunked writes

### 4. Spotify Match Quality
**Problem**: Fuzzy matching can return wrong tracks
**Solution**:
- Inspect `spotify_match_score` column
- Filter matches below threshold (e.g., < 0.6)
- Manual review of low-confidence matches

### 5. Memory Issues
**Problem**: Loading 50k+ row CSVs into memory
**Solution**:
- Use pandas chunking for very large datasets
- Consider SQLite or DuckDB for analysis
- Stream processing where possible

### 6. Docker PostgreSQL Persistence
**Problem**: Docker container destruction loses imported data
**Solution**:
- Use named volumes: `-v musicbrainz-data:/var/lib/postgresql/data`
- Document backup procedures
- Consider managed PostgreSQL for production

---

## 🔧 Environment Configuration

### Required Environment Variables
```bash
# Spotify API credentials (REQUIRED for enrichment)
SPOTIFY_CLIENT_ID=<your_client_id>
SPOTIFY_CLIENT_SECRET=<your_client_secret>

# Optional: Spotify redirect URI (not used in client credentials flow)
SPOTIFY_REDIRECT_URI=http://localhost:8888/callback

# Optional: MusicBrainz database connection
MUSICBRAINZ_DB_URL=postgresql://user:pass@localhost:5432/musicbrainz
```

### Getting Spotify Credentials
1. Visit https://developer.spotify.com/dashboard
2. Create a new app
3. Note Client ID and Client Secret
4. Add redirect URI (http://localhost:8888/callback)
5. Save credentials in `.env` file

### Docker Configuration
If using Docker for PostgreSQL:
```bash
# Default settings (can override via CLI)
CONTAINER_NAME=musicbrainz-postgres
DOCKER_IMAGE=postgres:14
POSTGRES_USER=musicbrainz
POSTGRES_PASSWORD=musicbrainz
POSTGRES_DB=musicbrainz
POSTGRES_PORT=5432
```

---

## 🧪 Testing & Validation

### Quick Smoke Test
```bash
# Test with a smaller artist first
python main.py --artist "Nick Cave" --get-covers --enrich-spotify --log-level INFO

# Verify outputs
ls -lh data/
cat data/dylan_works.csv | wc -l
```

### Data Quality Checks
```python
import pandas as pd

# Load enriched covers
df = pd.read_csv("data/dylan_covers_with_popularity.csv")

# Check Spotify match rate
match_rate = df["spotify_track_id"].notna().sum() / len(df)
print(f"Spotify match rate: {match_rate:.1%}")

# Check low-confidence matches
low_conf = df[df["spotify_match_score"] < 0.6]
print(f"Low confidence matches: {len(low_conf)}")

# Most popular covers
top_covers = df.nlargest(20, "spotify_popularity")
print(top_covers[["work_title", "cover_artist_name", "spotify_popularity"]])
```

### Common Validation Queries
```python
# Which Dylan songs have the most covers?
covers_per_song = df.groupby("work_title").size().sort_values(ascending=False)

# Which artists cover Dylan the most?
covers_per_artist = df.groupby("cover_artist_name").size().sort_values(ascending=False)

# Distribution of cover recordings over time
df["year"] = pd.to_datetime(df["first_release_date"], errors="coerce").dt.year
covers_by_year = df.groupby("year").size()
```

---

## 🚀 Future Enhancements

### Planned Improvements
1. **Audio Features**: Extract tempo, key, valence from Spotify
2. **Genre Clustering**: Group artists by musical style
3. **Network Analysis**: Graph visualization of cover relationships
4. **Time Series**: Track popularity changes over time
5. **Live Performance Data**: Add concert/festival cover data
6. **Lyrics Analysis**: NLP on cover interpretations
7. **YouTube Integration**: Add video cover metadata

### Technical Debt
1. Add comprehensive unit tests (pytest)
2. Implement proper database migrations
3. Add CLI progress bars (tqdm)
4. Create Docker Compose setup
5. Add data versioning (DVC)
6. Implement incremental updates (don't re-fetch everything)
7. Add .gitignore for data/ directory and .env files

### Scalability Considerations
- Currently single-threaded; could parallelize Spotify lookups
- Consider switching to async/await for API calls
- Implement database indexing for faster queries
- Add caching layer (Redis) for Spotify results
- Consider migrating to Apache Airflow for orchestration

---

## 📚 External Resources

### MusicBrainz
- **Database dumps**: https://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/
- **API docs**: https://musicbrainz.org/doc/MusicBrainz_API
- **Schema**: https://musicbrainz.org/doc/MusicBrainz_Database
- **Rate limit policy**: https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting

### Spotify
- **Web API docs**: https://developer.spotify.com/documentation/web-api
- **Client credentials flow**: https://developer.spotify.com/documentation/web-api/tutorials/client-credentials-flow
- **Track endpoints**: https://developer.spotify.com/documentation/web-api/reference/get-track

### Python Libraries
- **pandas**: https://pandas.pydata.org/docs/
- **requests**: https://requests.readthedocs.io/
- **SQLAlchemy**: https://docs.sqlalchemy.org/

---

## 🤖 AI Assistant Guidelines

### When Modifying the Parser
- **NEVER add limits to cover counts** - always paginate through all results
- Test API rate limiting with `--log-level DEBUG`
- Verify both DB and API code paths work
- Check that CSV schemas remain consistent

### When Modifying the Enricher
- Validate fuzzy matching accuracy with sample data
- Monitor Spotify API quota usage
- Test cache effectiveness
- Ensure match_score is always calculated

### When Adding Features
- Follow existing dataclass config pattern
- Add CLI arguments to `build_argument_parser()`
- Update this CLAUDE.md file with new workflows
- Consider backward compatibility with existing CSV files

### When Debugging
1. Check logs first (`--log-level DEBUG`)
2. Verify API credentials are valid
3. Test with smaller dataset (different artist)
4. Validate CSV output schemas
5. Check for rate limit errors (503, 429)

### When Optimizing
- Profile with `cProfile` before optimizing
- Consider local DB over API calls
- Implement caching for expensive operations
- Parallelize independent API calls

### Common User Requests
- **"Why are some songs missing?"** → Check for the 100-cover cap bug
- **"It's too slow"** → Recommend local DB setup
- **"Spotify matches are wrong"** → Lower the match_score threshold
- **"Out of memory"** → Suggest chunked CSV processing
- **"Rate limited"** → Increase sleep_seconds in config

---

## 📝 Maintenance Checklist

### Weekly
- [ ] Monitor MusicBrainz for schema changes
- [ ] Check Spotify API deprecation notices
- [ ] Review error logs for new failure patterns

### Monthly
- [ ] Update MusicBrainz database dump (if using local DB)
- [ ] Refresh Spotify tokens if using long-lived sessions
- [ ] Validate data quality metrics

### Quarterly
- [ ] Update Python dependencies (`pip list --outdated`)
- [ ] Re-run full pipeline to catch data drift
- [ ] Archive historical datasets

### Annually
- [ ] Major dependency upgrades (pandas, SQLAlchemy)
- [ ] MusicBrainz schema migrations
- [ ] Performance benchmarking and optimization

---

## 🎸 Project Context

This project analyzes Bob Dylan's cultural influence through the lens of cover songs. Dylan is one of the most covered artists in music history, with thousands of artists reinterpreting his work across decades and genres.

### Key Insights Expected
- Most covered Dylan songs (likely "Blowin' in the Wind", "Like a Rolling Stone")
- Temporal patterns (which decades saw most covers)
- Genre diversity (folk, rock, country, jazz interpretations)
- Geographic spread (international vs. US-based covers)
- Popularity vs. coverage correlation (popular songs != most covered)

### Analytical Questions to Answer
1. Which Dylan songs have the most covers?
2. Which artists cover Dylan the most?
3. How has cover frequency changed over time?
4. Do popular covers correlate with original popularity?
5. What's the average time lag between original and first cover?
6. Which Dylan albums are most covered?
7. Do certain covers introduce songs to new audiences?

---

**Last Updated**: 2026-05-05
**Version**: 1.0
**Maintainer**: Claude Code AI Assistant
