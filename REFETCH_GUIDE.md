# Selective Re-Fetch Guide

## Problem Identified

Analysis of `previous_iterations/` data revealed **208 songs capped at exactly 100 covers**, including:
- "Blowin' in the Wind" (103 → should be ~500+)
- "Like a Rolling Stone" (107 → should be ~400+)
- "All Along the Watchtower" (100 → should be ~300+)
- ... and 205 more songs

**Total missing covers: ~1,500-2,000+**

## Solution: Selective Re-Fetch

Instead of re-fetching all 650+ Dylan works (days), we re-fetch ONLY the 208 capped songs (hours).

---

## Step 1: Test with One Song (5 minutes)

Validate the fix works before processing all 208 songs:

```bash
# Activate virtual environment
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Test re-fetch with ONE capped song
python refetch_capped_songs.py \
    --limit 1 \
    --output-dir data

# Expected output:
#   Old count: 100 covers
#   New count: 400+ covers ✓
```

**If this works, proceed to Step 2. If not, debug before continuing.**

---

## Step 2: Re-Fetch All Capped Songs (~2-4 hours)

```bash
# Option A: Re-fetch all 208 songs using MusicBrainz API
python refetch_capped_songs.py \
    --output-dir data

# Option B: Use local MusicBrainz database (if you set it up)
python refetch_capped_songs.py \
    --db-url "postgresql://musicbrainz:musicbrainz@localhost:5432/musicbrainz" \
    --output-dir data

# Option C: Resume from song #50 if interrupted
python refetch_capped_songs.py \
    --start-from 50 \
    --output-dir data
```

**Output:** `data/refetched_recordings.csv` (~1,500-2,000+ new covers)

**Checkpoints:** Saves every 10 songs to `data/refetched_recordings_checkpoint_N.csv`

---

## Step 3: Merge Old + New Data (1 minute)

Combine:
- Old non-capped songs (kept as-is)
- Newly re-fetched capped songs (replaces old data)

```bash
python merge_old_and_new.py \
    --old-data previous_iterations/dylan_recordings_output.csv \
    --new-data data/refetched_recordings.csv \
    --capped-list capped_songs_to_refetch.csv \
    --output-dir data
```

**Outputs:**
- `data/dylan_recordings_merged.csv` - All recordings (Dylan + covers)
- `data/dylan_covers_merged.csv` - Cover versions only

**Expected stats:**
```
Old dataset (capped):         42,465 recordings
New merged dataset:           ~44,000+ recordings
  Gain:                       +1,500-2,000 recordings
  Percentage increase:        +3-5%
```

---

## Step 4: Validate Results (5 minutes)

```bash
python3 << 'EOF'
import pandas as pd

# Load merged covers
df = pd.read_csv('data/dylan_covers_merged.csv')

# Check top covered songs
print("=== TOP 20 MOST COVERED SONGS ===\n")
top_20 = df.groupby('work_title').size().sort_values(ascending=False).head(20)
for song, count in top_20.items():
    print(f"{song[:50]:<50} {count:>4} covers")

# Validation checks
print("\n=== VALIDATION CHECKS ===")
checks = [
    ("Blowin' in the Wind", 400),
    ("Like a Rolling Stone", 300),
    ("All Along the Watchtower", 250),
    ("Mr. Tambourine Man", 200),
]

for song, expected in checks:
    actual = len(df[df['work_title'] == song])
    status = "✅ PASS" if actual >= expected else "❌ FAIL"
    print(f"{status} {song}: {actual} covers (expected >={expected})")
EOF
```

**All checks should PASS.** If not, investigate which songs are still capped.

---

## Step 5: Enrich with Spotify (~2-4 hours)

Now enrich the complete merged dataset with Spotify data:

```bash
# Set up Spotify credentials if not already done
cat > .env << EOF
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
EOF

# Run Spotify enrichment on merged covers
python main.py \
    --enrich-spotify \
    --data-dir data
```

**Note:** You'll need to modify `spotify_enricher.py` to read from `dylan_covers_merged.csv` instead of `dylan_covers.csv`, or copy:
```bash
cp data/dylan_covers_merged.csv data/dylan_covers.csv
python main.py --enrich-spotify
```

**Output:** `data/dylan_covers_with_popularity.csv`

---

## Step 6: Merge Enrichment Metadata (Optional)

The previous iteration has valuable enrichment (genre, era, themes). Merge this into the new dataset:

```bash
python3 << 'EOF'
import pandas as pd

# Load new covers and old enriched data
new_df = pd.read_csv('data/dylan_covers_with_popularity.csv')
old_enriched = pd.read_csv('previous_iterations/dylan_enriched.csv')

# Merge on work_id and recording_id to preserve enrichment
enrichment_cols = ['genre', 'subgenre', 'dylan_era', 'theme', 'cultural_context', 
                   'cover_significance', 'influence_score']

# Select only enrichment columns from old data
old_enrichment = old_enriched[['work_id', 'recording_id'] + enrichment_cols].drop_duplicates()

# Merge with new data (left join - keeps all new data, adds enrichment where available)
final_df = new_df.merge(
    old_enrichment,
    on=['work_id', 'recording_id'],
    how='left'
)

# Save final enriched dataset
final_df.to_csv('data/dylan_covers_final_enriched.csv', index=False)

print(f"✅ Merged enrichment data")
print(f"   Total covers: {len(final_df)}")
print(f"   With enrichment: {final_df['genre'].notna().sum()} ({final_df['genre'].notna().sum() / len(final_df) * 100:.1f}%)")
EOF
```

---

## Troubleshooting

### "Module not found: pandas"
```bash
pip install -r requirements.txt
```

### MusicBrainz rate limiting (503 errors)
- Script respects 1 req/sec limit automatically
- If you hit persistent rate limits, use local database (--db-url option)

### Checkpoint recovery
If interrupted at song #N:
```bash
python refetch_capped_songs.py --start-from N
```

### Validation fails (songs still capped)
1. Check MusicBrainz manually: https://musicbrainz.org/work/{work_id}
2. Some songs may legitimately have ~100 covers
3. Re-run specific songs: edit `capped_songs_to_refetch.csv` to only include failed songs

---

## Files Created

| File | Description | Size |
|------|-------------|------|
| `capped_songs_to_refetch.csv` | List of 208 capped work IDs | ~10 KB |
| `refetch_capped_songs.py` | Re-fetch script | ~5 KB |
| `merge_old_and_new.py` | Merge script | ~4 KB |
| `data/refetched_recordings.csv` | Newly fetched covers | ~2 MB |
| `data/dylan_recordings_merged.csv` | All recordings merged | ~15 MB |
| `data/dylan_covers_merged.csv` | Cover recordings only | ~13 MB |
| `data/dylan_covers_with_popularity.csv` | + Spotify data | ~14 MB |
| `data/dylan_covers_final_enriched.csv` | + Genre/themes | ~20 MB |

---

## Timeline Estimate

| Step | Time | Notes |
|------|------|-------|
| 1. Test one song | 5 min | Validate fix works |
| 2. Re-fetch 208 songs | 2-4 hrs | ~1 req/sec rate limit |
| 3. Merge datasets | 1 min | Fast pandas operation |
| 4. Validate | 5 min | Check key songs |
| 5. Spotify enrichment | 2-4 hrs | Depends on API rate limits |
| 6. Merge enrichment | 5 min | Optional pandas merge |
| **TOTAL** | **~5-9 hours** | Mostly automated |

---

## Next Steps After Completion

1. ✅ Validate all checks pass
2. ✅ Create visualizations comparing old vs. new counts
3. ✅ Update documentation with final statistics
4. ✅ Archive previous_iterations/ as historical reference
5. ✅ Publish final dataset for analysis

---

**Created**: 2026-05-05  
**Status**: Ready to execute  
**Estimated savings**: Days → Hours by avoiding full re-fetch
