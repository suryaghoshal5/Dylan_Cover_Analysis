import psycopg2
import pandas as pd
from pathlib import Path

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="musicbrainz",
    password="musicbrainz",
    database="musicbrainz"
)

print("Extracting Dylan works...")

# Get all Dylan works
works_query = """
SELECT DISTINCT
    w.id,
    w.gid::text as work_id,
    w.name as title,
    wt.name as work_type
FROM musicbrainz.work w
LEFT JOIN musicbrainz.work_type wt ON w.type = wt.id
JOIN musicbrainz.l_artist_work law ON law.entity1 = w.id
WHERE law.entity0 = 17
ORDER BY w.name;
"""

works_df = pd.read_sql(works_query, conn)
print(f"Found {len(works_df)} Dylan works")

# Get all recordings
print("Extracting recordings...")

recordings_query = """
SELECT
    w.gid::text as work_id,
    w.name as work_title,
    r.gid::text as recording_id,
    r.name as recording_title,
    r.length as recording_length_ms,
    ac.name as artist_names,
    CASE WHEN EXISTS (
        SELECT 1 FROM musicbrainz.artist_credit_name acn
        WHERE acn.artist_credit = r.artist_credit AND acn.artist = 17
    ) THEN true ELSE false END as is_bob_dylan
FROM musicbrainz.work w
JOIN musicbrainz.l_recording_work lrw ON lrw.entity1 = w.id
JOIN musicbrainz.recording r ON r.id = lrw.entity0
JOIN musicbrainz.artist_credit ac ON r.artist_credit = ac.id
JOIN musicbrainz.l_artist_work law ON law.entity1 = w.id
WHERE law.entity0 = 17;
"""

recordings_df = pd.read_sql(recordings_query, conn)
print(f"Found {len(recordings_df)} total recordings")

# Separate covers
covers_df = recordings_df[recordings_df['is_bob_dylan'] == False].copy()
print(f"Found {len(covers_df)} covers")

# Save
Path("data").mkdir(exist_ok=True)
works_df.to_csv("data/dylan_works.csv", index=False)
recordings_df.to_csv("data/dylan_recordings.csv", index=False)
covers_df.to_csv("data/dylan_covers.csv", index=False)

print("\n✅ Extraction complete!")
print(f"   Works: data/dylan_works.csv")
print(f"   All recordings: data/dylan_recordings.csv")
print(f"   Covers only: data/dylan_covers.csv")

# Show top covered songs
print("\nTop 20 most covered Dylan songs:")
top_covered = covers_df.groupby('work_title').size().sort_values(ascending=False).head(20)
for song, count in top_covered.items():
    print(f"  {song[:50]:<50} {count:>5} covers")

conn.close()
