"""Spotify enrichment with reuse of previous matches.

Strategy:
1. Load new deduplicated covers (10,444 rows)
2. Load previous enriched data (20,631 rows) and extract Spotify columns
3. Join on recording_id to carry over existing matches
4. Only call Spotify API for unmatched covers (using cascading fuzzy matcher)
5. Apply match score threshold (0.6) to filter false positives
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from spotify_enricher import SpotifyConfig, SpotifyEnricher

load_dotenv()

MAIN_REPO = Path(os.path.expanduser(
    "~/Library/CloudStorage/Dropbox/Interest - Non Work/Dylan_Cover_Analysis"
))
DATA_DIR = Path("data")

SPOTIFY_COLS = [
    "spotify_track_id", "spotify_track_name", "spotify_artist_name",
    "spotify_popularity", "spotify_album_name", "spotify_release_date",
    "spotify_duration_ms", "spotify_is_explicit", "spotify_external_url",
    "spotify_match_score", "spotify_match_tier",
]

# ── Step 1: Load new covers ─────────────────────────────────────────
covers_df = pd.read_csv(DATA_DIR / "dylan_covers.csv")
print(f"New covers: {len(covers_df)}")

# ── Step 2: Load previous Spotify data ──────────────────────────────
prev_path = MAIN_REPO / "data" / "dylan_covers_with_popularity.csv"
prev_df = pd.read_csv(prev_path)
print(f"Previous enriched rows: {len(prev_df)}")

available_cols = ["recording_id"] + [c for c in SPOTIFY_COLS if c in prev_df.columns]
prev_spotify = prev_df[available_cols].copy()
prev_spotify = prev_spotify.drop_duplicates(subset="recording_id", keep="first")

if "spotify_match_score" in prev_spotify.columns:
    low_score = prev_spotify["spotify_match_score"].notna() & (prev_spotify["spotify_match_score"] < 0.6)
    n_filtered = low_score.sum()
    for col in SPOTIFY_COLS:
        if col in prev_spotify.columns:
            prev_spotify.loc[low_score, col] = None
    print(f"Filtered {n_filtered} previous matches below 0.6 threshold")

prev_matched = prev_spotify[prev_spotify["spotify_track_id"].notna()].copy()
print(f"Previous Spotify matches (reusable): {len(prev_matched)}")

# ── Step 3: Join existing matches ───────────────────────────────────
merged = covers_df.merge(prev_matched, on="recording_id", how="left")
already_matched = merged["spotify_track_id"].notna().sum()
needs_lookup = merged["spotify_track_id"].isna().sum()
print(f"Matched from previous: {already_matched}")
print(f"Need Spotify lookup: {needs_lookup}")

# ── Step 4: Enrich only unmatched rows ──────────────────────────────
if needs_lookup > 0:
    config = SpotifyConfig(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        data_dir=DATA_DIR,
        min_match_score=0.6,
    )
    enricher = SpotifyEnricher(config)

    unmatched_idx = merged.index[merged["spotify_track_id"].isna()]
    base_cols = [c for c in covers_df.columns]
    unmatched_df = merged.loc[unmatched_idx, base_cols].copy()

    print(f"\nEnriching {len(unmatched_df)} unmatched covers via Spotify API (cascading fuzzy match)...")
    enriched_new = enricher.enrich_dataframe(unmatched_df)

    for col in SPOTIFY_COLS:
        if col in enriched_new.columns:
            merged.loc[unmatched_idx, col] = enriched_new[col].values

new_matches = merged["spotify_track_id"].notna().sum()
print(f"\nFinal Spotify matches: {new_matches}/{len(merged)} ({new_matches/len(merged):.1%})")

# ── Step 5: Save ────────────────────────────────────────────────────
output_path = DATA_DIR / "dylan_covers_with_popularity.csv"
merged.to_csv(output_path, index=False)
print(f"Saved to {output_path}")

# Quality summary
print(f"\n── Enrichment Quality ──")
print(f"  Total covers: {len(merged)}")
print(f"  Spotify matched: {new_matches} ({new_matches/len(merged):.1%})")
print(f"  Reused from previous: {already_matched}")
print(f"  Newly matched: {new_matches - already_matched}")
if "spotify_match_score" in merged.columns:
    matched = merged[merged["spotify_match_score"].notna()]
    if not matched.empty:
        print(f"  Mean match score: {matched['spotify_match_score'].mean():.3f}")
        print(f"  Min match score: {matched['spotify_match_score'].min():.3f}")
if "spotify_match_tier" in merged.columns:
    tier_counts = merged["spotify_match_tier"].value_counts()
    if not tier_counts.empty:
        print(f"  Match tiers: {dict(tier_counts)}")
