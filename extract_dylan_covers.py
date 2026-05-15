"""Extract Dylan works, recordings and covers from local MusicBrainz PostgreSQL.

Addresses QC findings:
- C1: Deduplicates by recording_id (was inflated ~2x from release/link joins)
- C2: Includes all documented columns (artist_ids, release_id, first_release_date, etc.)
- C3: Works schema matches documentation (work_id, title, type, iswc, ...)
- C4: All downstream counts use unique recording_ids

Strategy: fast SQL queries for core data, then pandas for dedup and enrichment.
"""

import psycopg2
import pandas as pd
from pathlib import Path

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="musicbrainz",
    password="musicbrainz",
    database="musicbrainz",
)

DYLAN_ARTIST_ID = 17

# ── Step 1: Works ────────────────────────────────────────────────────
print("Step 1/4: Extracting Dylan works...")

works_query = """
SELECT DISTINCT
    w.gid::text AS work_id,
    w.name AS title,
    wt.name AS type,
    (SELECT i.iswc FROM musicbrainz.iswc i WHERE i.work = w.id LIMIT 1) AS iswc,
    w.comment AS disambiguation
FROM musicbrainz.work w
LEFT JOIN musicbrainz.work_type wt ON w.type = wt.id
JOIN musicbrainz.l_artist_work law ON law.entity1 = w.id
WHERE law.entity0 = %(artist_id)s
ORDER BY w.name;
"""

works_df = pd.read_sql(works_query, conn, params={"artist_id": DYLAN_ARTIST_ID})
print(f"  Found {len(works_df)} Dylan works")

# ── Step 2: Core recordings (fast query, may have dupes) ────────────
print("Step 2/4: Extracting recordings...")

recordings_query = """
SELECT
    w.gid::text AS work_id,
    w.name AS work_title,
    r.gid::text AS recording_id,
    r.name AS recording_title,
    r.length AS recording_length_ms,
    ac.name AS artist_names,
    CASE WHEN EXISTS (
        SELECT 1 FROM musicbrainz.artist_credit_name acn
        WHERE acn.artist_credit = r.artist_credit AND acn.artist = %(artist_id)s
    ) THEN true ELSE false END AS is_bob_dylan
FROM musicbrainz.work w
JOIN musicbrainz.l_recording_work lrw ON lrw.entity1 = w.id
JOIN musicbrainz.recording r ON r.id = lrw.entity0
JOIN musicbrainz.artist_credit ac ON r.artist_credit = ac.id
JOIN musicbrainz.l_artist_work law ON law.entity1 = w.id
WHERE law.entity0 = %(artist_id)s;
"""

recordings_df = pd.read_sql(recordings_query, conn, params={"artist_id": DYLAN_ARTIST_ID})
pre_dedup = len(recordings_df)
recordings_df = recordings_df.drop_duplicates(subset="recording_id", keep="first")
print(f"  Found {pre_dedup} raw rows → {len(recordings_df)} unique recordings (removed {pre_dedup - len(recordings_df)} dupes)")

# ── Step 3: Enrich with artist_ids, release info, ISRCs ─────────────
print("Step 3/4: Enriching with artist_ids, release info, ISRCs...")

# Get internal recording IDs for enrichment joins
rec_gids = recordings_df["recording_id"].tolist()

# 3a: artist_ids
print("  Fetching artist_ids...")
artist_ids_query = """
SELECT
    r.gid::text AS recording_id,
    string_agg(a.gid::text, ';' ORDER BY a.name) AS artist_ids
FROM musicbrainz.recording r
JOIN musicbrainz.artist_credit_name acn ON acn.artist_credit = r.artist_credit
JOIN musicbrainz.artist a ON a.id = acn.artist
WHERE r.gid::text = ANY(%(gids)s)
GROUP BY r.gid;
"""
artist_ids_df = pd.read_sql(artist_ids_query, conn, params={"gids": rec_gids})
recordings_df = recordings_df.merge(artist_ids_df, on="recording_id", how="left")

# 3b: earliest release info
print("  Fetching release info (earliest per recording)...")
release_query = """
SELECT DISTINCT ON (r.gid)
    r.gid::text AS recording_id,
    rel.gid::text AS release_id,
    rel.name AS release_title,
    rc.date_year AS rel_year,
    rc.date_month AS rel_month,
    rc.date_day AS rel_day
FROM musicbrainz.recording r
JOIN musicbrainz.track t ON t.recording = r.id
JOIN musicbrainz.medium m ON m.id = t.medium
JOIN musicbrainz.release rel ON rel.id = m.release
LEFT JOIN musicbrainz.release_country rc ON rc.release = rel.id
WHERE r.gid::text = ANY(%(gids)s)
ORDER BY r.gid, rc.date_year NULLS LAST, rc.date_month NULLS LAST, rc.date_day NULLS LAST;
"""
release_df = pd.read_sql(release_query, conn, params={"gids": rec_gids})

# Build first_release_date string
def make_date_str(row):
    y = row.get("rel_year")
    if pd.isna(y) or y is None:
        return None
    y = int(y)
    m = int(row["rel_month"]) if pd.notna(row.get("rel_month")) else 1
    d = int(row["rel_day"]) if pd.notna(row.get("rel_day")) else 1
    return f"{y:04d}-{m:02d}-{d:02d}"

release_df["first_release_date"] = release_df.apply(make_date_str, axis=1)
release_df = release_df.drop(columns=["rel_year", "rel_month", "rel_day"])
recordings_df = recordings_df.merge(release_df, on="recording_id", how="left")

# 3c: ISRCs
print("  Fetching ISRCs...")
isrc_query = """
SELECT
    r.gid::text AS recording_id,
    string_agg(i.isrc, ';') AS isrcs
FROM musicbrainz.recording r
JOIN musicbrainz.isrc i ON i.recording = r.id
WHERE r.gid::text = ANY(%(gids)s)
GROUP BY r.gid;
"""
isrc_df = pd.read_sql(isrc_query, conn, params={"gids": rec_gids})
recordings_df = recordings_df.merge(isrc_df, on="recording_id", how="left")

conn.close()

# ── Step 4: Split covers and save ───────────────────────────────────
print("Step 4/4: Identifying covers and saving...")

covers_df = recordings_df[~recordings_df["is_bob_dylan"]].copy()
covers_df["cover_artist_name"] = covers_df["artist_names"]
covers_df["cover_artist_ids"] = covers_df["artist_ids"]
print(f"  {len(covers_df)} cover recordings")

Path("data").mkdir(exist_ok=True)
works_df.to_csv("data/dylan_works.csv", index=False)
recordings_df.to_csv("data/dylan_recordings.csv", index=False)
covers_df.to_csv("data/dylan_covers.csv", index=False)

print(f"\nExtraction complete!")
print(f"  Works: data/dylan_works.csv ({len(works_df)} rows)")
print(f"  All recordings: data/dylan_recordings.csv ({len(recordings_df)} rows)")
print(f"  Covers only: data/dylan_covers.csv ({len(covers_df)} rows)")

# Top covered songs
print("\nTop 20 most covered Dylan songs (unique recordings):")
top_covered = (
    covers_df.groupby("work_title")["recording_id"]
    .nunique()
    .sort_values(ascending=False)
    .head(20)
)
for song, count in top_covered.items():
    print(f"  {song[:50]:<50} {count:>5} covers")

print("\nTop 10 by unique artists (cultural reach):")
top_artists = (
    covers_df.groupby("work_title")["artist_names"]
    .nunique()
    .sort_values(ascending=False)
    .head(10)
)
for song, count in top_artists.items():
    print(f"  {song[:50]:<50} {count:>5} artists")

# Data quality summary
print("\n── Data Quality ──")
print(f"  Columns: {list(covers_df.columns)}")
print(f"  first_release_date coverage: {covers_df['first_release_date'].notna().sum()}/{len(covers_df)} ({covers_df['first_release_date'].notna().mean():.1%})")
print(f"  artist_ids coverage: {covers_df['artist_ids'].notna().sum()}/{len(covers_df)} ({covers_df['artist_ids'].notna().mean():.1%})")
print(f"  release_id coverage: {covers_df['release_id'].notna().sum()}/{len(covers_df)} ({covers_df['release_id'].notna().mean():.1%})")
print(f"  isrcs coverage: {covers_df['isrcs'].notna().sum()}/{len(covers_df)} ({covers_df['isrcs'].notna().mean():.1%})")
