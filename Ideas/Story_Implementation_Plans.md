# Dylan Cover Analysis — Implementation Plans for All Stories

Concrete engineering plans for the six story concepts in `Dylan_Story_Concepts.md`. Each plan lists the data work, analyses, deliverables, and risks. The shared foundation (Phase 0) must complete before any individual story plan is reliable.

---

## Phase 0 — Shared Foundation (blocks all stories)

These tasks underpin every story. Do them once.

1. **Resolve the cover cap bug.** Confirm whether `refetch_capped_songs.py` has been run for all 201 capped works. Validate top-20 cover counts match expected ranges in `CLAUDE.md`.
2. **Build canonical cover table** `data/dylan_covers_with_popularity.csv` per the schema in `CLAUDE.md`.
3. **Merge prior enrichment.** Left-join `previous_iterations/dylan_enriched.csv` on `recording_id` to inherit `genre`, `subgenre`, `theme`, `cultural_context`, `influence_score`, `dylan_era`.
4. **Quality report** `data/quality_report.md`: row counts, null rates, Spotify match rate, low-confidence match count, top-50 cover counts vs. expectations.
5. **Decide on tempo/BPM source.** Spotify `audio_features` is deprecated for new apps (Nov 2024). Options: cached features from earlier runs, AcousticBrainz dumps, or drop BPM claims. Document the decision in `data/audio_features_status.md` — multiple stories depend on this.

Deliverables: validated master CSV, quality report, audio-features decision.

---

## Story 1 — "Why Musicians Can't Stop Covering Bob Dylan"

**Thesis:** Dylan songs are structurally unfinished, inviting reinterpretation.

### Data work
- Cover-count distribution for all Dylan works; identify top 50.
- Comparable cover counts for Beatles and Rolling Stones works (re-run `musicbrainz_parser.py` with `--artist`). Cap-bug fix must apply.
- Genre breadth per song: count of distinct `genre` values from enriched data per top-50 work.

### Analyses (`analysis/story1_coverability.ipynb`)
- Lorenz curve / Gini of covers across Dylan's catalog (concentration vs. spread).
- Dylan vs. Beatles vs. Stones: covers-per-song distribution (boxplot + medians).
- Genre breadth correlation: does a song's cover count correlate with genre diversity?
- Time-to-first-cover and cover half-life per song.

### Deliverables
- `analysis/figures/coverability_lorenz.png`
- `analysis/figures/dylan_vs_beatles_stones.png`
- `analysis/story1_findings.md` — quantified claims usable in the article.

### Risks
- Beatles/Stones comparison requires re-running the pipeline for two more artists; budget ~12h API or local-DB query.
- "Chord progression / lyrical ambiguity" claims are musicological and not in our data — flag as interview/editorial.

---

## Story 2 — "All Along the Watchtower: The Song That Changed 300 Times" *(primary pitch)*

**Thesis:** One song's 300+ covers map the evolution of musical interpretation.

### Data work
- Filter master CSV to `work_title == "All Along the Watchtower"`; verify count ≥ 300 (cap bug check).
- For each cover: artist, year, genre/subgenre, `recording_length_ms`, Spotify popularity, country (from artist MusicBrainz lookup if available).
- Manual annotation of ~20 canonical versions (Hendrix, U2, DMB, Vedder, etc.) — see `data/watchtower_canonical.csv` (new, hand-curated).

### Analyses (`analysis/story2_watchtower.ipynb`)
- Duration distribution: original 2:31 vs. cover spread; identify min/max.
- Genre trajectory: stacked area chart of genres by release year.
- Popularity vs. fidelity: Spotify popularity against duration deviation from original.
- "Hendrix watershed" test: do post-1968 covers diverge more (in duration/genre) than pre-1968?
- Cover cadence: covers-per-year time series with annotations for canonical versions.

### Deliverables
- `analysis/figures/watchtower_timeline.png`
- `analysis/figures/watchtower_genre_evolution.png`
- `analysis/figures/watchtower_duration_spread.png`
- `analysis/story2_findings.md`
- Optional: web embed JSON for an interactive timeline.

### Risks
- Audio feature data (key, tempo) likely unavailable — see Phase 0 decision. Without it, claims about "key changes" must be removed or sourced manually.
- Country/region tagging requires artist-level MusicBrainz lookups (extra API budget).

---

## Story 3 — "How Punk Killed the Reverential Cover"

**Thesis:** Punk inaugurated deconstruction; cover fidelity collapses post-1976.

### Data work
- Define a **fidelity proxy** (since we likely lack BPM): combine genre-distance from original (folk) + duration deviation. Document the formula in `analysis/fidelity_metric.md`.
- Tag each cover with decade and genre family (folk / rock / punk / metal / hip-hop / electronic / jazz / country / world).

### Analyses (`analysis/story3_punk_shift.ipynb`)
- Fidelity-proxy distribution by decade (boxplot).
- Genre-family share per decade (stacked bar).
- Change-point detection on yearly mean fidelity (rupture, PELT) — does 1976–1980 show a structural break?
- Same analysis restricted to the top-20 most-covered songs to control for catalog noise.

### Deliverables
- `analysis/figures/fidelity_by_decade.png`
- `analysis/figures/genre_share_by_decade.png`
- `analysis/story3_findings.md` with explicit caveats about the proxy.

### Risks
- The thesis hinges on tempo/arrangement data we don't have. Without BPM, the article becomes a genre-share story; either accept that or sample 50 covers for manual musicological coding.
- "TikTok 2020s fragmentation" is not in MusicBrainz; flag as editorial.

---

## Story 4 — "The Protest Song Lifecycle: A 60-Year Study" *(secondary pitch)*

**Thesis:** Protest covers musically mirror their era's protest aesthetic.

### Data work
- Verify completeness for "Masters of War", "Blowin' in the Wind", "The Times They Are A-Changin'" (all three were capped).
- Build `data/political_events.csv` (hand-curated, ~30 rows): event, date, type (war / movement / election).
- Define eras: Vietnam 1963–1975, Punk/Reagan 1976–1989, Post-9/11 2001–2009, Occupy/Trump 2011–2019, BLM/COVID 2020–present. Document in `analysis/era_definitions.md`.

### Analyses (`analysis/story4_protest_lifecycle.ipynb`)
- Covers/year per song with political-event overlay; z-score spike detection.
- Genre share per era, per song (small multiples).
- Top covering artists per era.
- Median duration drift per era.
- Spotify popularity by era (proxy for whether each era's covers "stuck").
- Lag analysis: years from 1963 original to first cover in each new genre family.

### Deliverables
- `analysis/figures/protest_timeline_with_events.png`
- `analysis/figures/protest_genre_by_era.png`
- `analysis/figures/protest_heatmap.png`
- `analysis/story4_findings.md`
- `data/political_events.csv`

### Risks
- "Tempo as anger" requires BPM — pivot to "genre as anger" if Phase 0 confirms no tempo data.
- Era boundaries are editorial; lock them before analysis, do not tune to story.
- Cap bug: original truncation may have systematically removed Vietnam-era covers; verify before drawing conclusions.

---

## Story 5 — "The American Songbook Doesn't Know It Needs Bob Dylan" *(October pitch)*

**Thesis:** Jazz/standards singers are canonizing Dylan, lagging public perception.

### Data work
- Tag covers as "standards-adjacent": genre ∈ {jazz, vocal jazz, easy listening, traditional pop, adult standards}. Use `genre`/`subgenre` from enrichment plus a manual allowlist.
- Compile list of "standards" artists covering Dylan (Bennett, Krall, Nelson, Streisand, Rod Stewart, Cassandra Wilson, etc.) — `data/standards_artists.csv`.

### Analyses (`analysis/story5_songbook.ipynb`)
- Time series of standards-adjacent covers as a share of total Dylan covers, 1963–present.
- Top 5 Dylan songs covered by standards singers; compare to top 5 overall.
- Decade-of-birth distribution of covering artists (proxy for generational shift) — requires artist DOB from MusicBrainz.
- Comparison reference: Gershwin/Porter cover counts per work (sanity check on "standard" definition).

### Deliverables
- `analysis/figures/standards_share_over_time.png`
- `analysis/figures/standards_top_songs.png`
- `analysis/story5_findings.md`

### Risks
- Genre labels in MusicBrainz are inconsistent — manual review of edge cases required.
- "Harmonic sophistication" claim is musicological, not in our data — flag for interview/expert quote.

---

## Story 6 — "The Geography of Interpretation"

**Thesis:** Cross-cultural Dylan covers reveal what's essential vs. contingent in a song.

### Data work
- Artist-country lookup via MusicBrainz artist endpoint (`area` field). New script `scripts/enrich_artist_country.py`; cache results.
- Language detection on `recording_title` (titles may be translated) — `langdetect` or `fasttext`.
- Region groupings: Latin America, East Asia, South Asia, Caribbean, West Africa, Europe-non-Anglophone, Anglophone.

### Analyses (`analysis/story6_geography.ipynb`)
- Cover count by country and region; choropleth.
- Genre profile per region (e.g., Jamaica → reggae share).
- Translated-title rate per region.
- Top non-Anglophone covers by Spotify popularity.
- Case-study filters: Brazil, Japan, Jamaica, India, West Africa — top 10 covers each.

### Deliverables
- `analysis/figures/world_cover_density.png`
- `analysis/figures/regional_genre_profile.png`
- `analysis/story6_findings.md`
- `data/artist_country.csv`

### Risks
- Artist-area data is missing for many MusicBrainz entries; expect 60–75% coverage. Document.
- Country ≠ cultural origin (e.g., a UK-based Brazilian artist). Flag in methodology.
- Title-language ≠ lyric-language. Lyrics not in dataset; either accept the proxy or scope down.

---

## Cross-Story Engineering Tasks

These show up in multiple plans; build once, reuse.

| Task | Used by | Output |
|---|---|---|
| Master cover CSV with enrichment merge | All | `data/dylan_covers_with_popularity.csv` |
| Genre-family normalizer | 1, 3, 4, 5, 6 | `analysis/lib/genre_family.py` |
| Era / decade tagger | 3, 4, 5 | `analysis/lib/eras.py` |
| Artist-country lookup | 2, 6 | `data/artist_country.csv` |
| Audio-features availability decision | 2, 3, 4 | `data/audio_features_status.md` |
| Political events table | 4 | `data/political_events.csv` |
| Comparison-artist pipeline runs (Beatles, Stones, Gershwin/Porter) | 1, 5 | `data/comparison_<artist>_covers.csv` |

---

## Recommended Execution Order

1. **Phase 0 foundation** (1–2 days).
2. **Story 2 deep-dive** — primary pitch, single-song scope keeps it tractable (3–4 days).
3. **Story 4 analysis** — secondary pitch, reuses era/genre infra (2–3 days).
4. **Story 1 comparison runs** — start Beatles/Stones pipeline in background as soon as Phase 0 is done.
5. **Story 3** — depends on fidelity-proxy decision; can run in parallel with 1.
6. **Story 5** — October pitch, no rush; build after May 24 deadline.
7. **Story 6** — largest data-acquisition burden (per-artist country lookups); start the lookup script early in background, defer analysis until late.

---

## Open Questions for the Editor / Author

- Is BPM/audio-feature analysis a hard requirement, or can genre/duration serve as proxies?
- For Story 1, are Beatles + Stones the right comparators, or include Joni Mitchell / Leonard Cohen?
- For Story 4, lock era boundaries — proposed dates above OK?
- For Story 6, scope: top 5 countries or full global treatment?
- Interview budget: how many artists per story, and who handles outreach?

---

**Last updated:** 2026-05-11
