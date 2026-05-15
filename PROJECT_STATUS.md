---
title: Dylan Cover Analysis - Project Status
date: 2026-05-12
status: Data Complete (QC Fixes Applied) - Ready for Analysis
updated: 2026-05-12T18:00:00
tags: [dylan, status-update, data-pipeline, qc-fixed]
---

# Dylan Cover Analysis - COMPLETE (QC-Corrected)

**Date:** May 12, 2026
**Status:** ALL DATA READY (post-QC)
**Deadline:** 12 days until Dylan's 85th birthday (May 24, 2026)

---

## QC Fixes Applied (2026-05-12)

Previous data had a **2x row inflation** bug — recordings were duplicated per-release. All numbers below are corrected.

| What changed | Before (inflated) | After (corrected) |
|---|---|---|
| Cover recordings | 20,631 | 10,444 |
| Total recordings | 61,193 | 31,041 |
| Spotify match rate | 66.8% | 92.8% |
| Missing columns | 7 columns absent | All present |
| Works schema | 4 columns, wrong names | 5 columns, documented schema |
| False positive matches | ~225 unfiltered | Filtered at 0.6 threshold |
| Match strategy | Single-tier | 4-tier cascading fuzzy match |

---

## Final Numbers

| Metric | Count |
|--------|-------|
| Dylan Works | 1,309 |
| Total Recordings (unique) | 31,041 |
| Cover Versions (unique) | 10,444 |
| Spotify Matched | 9,697 (92.8%) |

---

## Top 10 Most Covered Songs (Corrected)

1. **Knockin' on Heaven's Door** - 615 unique covers
2. **All Along the Watchtower** - 558 covers (Story #2)
3. **Mr. Tambourine Man** - 388 covers
4. **Blowin' in the Wind** - 343 covers
5. **I Shall Be Released** - 287 covers
6. **Like a Rolling Stone** - 281 covers
7. **Don't Think Twice, It's All Right** - 278 covers
8. **It's All Over Now, Baby Blue** - 257 covers
9. **This Wheel's on Fire** - 185 covers
10. **The Times They Are A-Changin'** - 177 covers

---

## Top 10 by Unique Artists (Cultural Reach)

1. **Knockin' on Heaven's Door** - 169 unique artists
2. **Blowin' in the Wind** - 166 artists
3. **All Along the Watchtower** - 164 artists
4. **Don't Think Twice, It's All Right** - 130 artists
5. **Mr. Tambourine Man** - 110 artists
6. **I Shall Be Released** - 108 artists
7. **Make You Feel My Love** - 100 artists
8. **The Times They Are A-Changin'** - 98 artists
9. **Like a Rolling Stone** - 96 artists
10. **It's All Over Now, Baby Blue** - 79 artists

---

## Most Popular on Spotify

- **Guns N' Roses** - "Knockin' on Heaven's Door" (83)
- **Darius Rucker** - "Wagon Wheel" (83)
- **Jimi Hendrix** - "All Along the Watchtower" (77)
- **The Animals** - "House of the Risin' Sun" (76)

---

## Column Coverage

| Column | Coverage |
|--------|----------|
| work_id / work_title | 100% |
| recording_id | 100% |
| artist_names / artist_ids | 100% |
| release_id / release_title | 99.6% |
| first_release_date | 78.0% |
| isrcs | 20.5% |
| spotify_* (all fields) | 92.8% |

---

## Spotify Match Tiers

| Tier | Matches | Description |
|------|---------|-------------|
| song+artist+album+year | 1,061 | Strictest match |
| song+artist+album | 614 | No year required |
| song+artist | 1,284 | Standard match |
| song-only | 161 | High title threshold (0.85) |
| Reused from previous | 6,577 | Carried over by recording_id |

---

## Known Limitations

- **Same-title false positives**: Some non-Dylan songs with identical titles (e.g., Alphaville's "Forever Young") may match. Affects ~10-20 rows.
- **ISRC coverage low** (20.5%): Many older recordings lack ISRCs in MusicBrainz.
- **first_release_date** at 78%: Some recordings have no associated release date.

---

## Next: Story #2 Analysis

**"All Along the Watchtower: The Song That Changed 558 Times"**

- 558 unique covers available (corrected from inflated 1,116)
- 164 unique artists
- Full Spotify data
- Target: NYT pitch by May 15

**Data location:** `data/dylan_covers_with_popularity.csv`

---

## What's Complete

- [x] MusicBrainz database imported (38M recordings)
- [x] Dylan works extracted (1,309)
- [x] Cover recordings identified and deduplicated (10,444)
- [x] QC fixes applied (dedup, schema, missing columns)
- [x] Cascading fuzzy matcher built (4-tier with standardization)
- [x] Spotify enrichment (92.8% match rate)
- [x] Final dataset ready (5.2MB)

---

## Timeline

- **May 11** - Pipeline complete (v1, had 2x inflation bug)
- **May 12** - QC fixes applied, data regenerated (v2)
- **May 15** - NYT pitch deadline
- **May 24** - Dylan's 85th birthday
