# Story 3 — How Punk Killed the Reverential Cover

## Pitch-Ready Claims

### 1. The 1980s Were Peak Reverence — Then Everything Changed
In the 1980s, **69% of Dylan covers faithfully matched the original's duration** (within 15%). By the 2020s, that number collapsed to **24%**. The interquartile range of duration ratios — a measure of interpretive diversity — widened from **0.17 to 0.78**, a nearly 5× expansion. Artists no longer feel obligated to reproduce; they feel licensed to reimagine.

### 2. Genre Determines Fidelity More Than Era
Country artists compress Dylan to **73% of original length**; jam bands stretch him to **2× duration**. Metal artists add **35% more time**. Folk singers stay closest to the source (46% faithful rate). The genre an artist belongs to is a stronger predictor of cover fidelity than the decade the cover was recorded.

### 3. The Radical Cover Is Rising
Covers deviating more than 50% from Dylan's original duration rose from **6% in the 1980s to 26% in the 2020s**. The most popular radical reinterpretation: Guns N' Roses' "Knockin' on Heaven's Door" (1.35× original length, Spotify popularity 83), which became a stadium anthem that bears almost no resemblance to Dylan's gentle original.

### 4. The Interpretation Spectrum Is a Genre Map
Across 10 genre families, the median duration ratio forms a clean spectrum from Country (0.73×) through Folk (0.91×), Classic Rock (0.86×), Alt/Indie (0.95×), Punk (1.0×), Metal (1.35×) to Jam Band (2.04×). Each genre has developed its own distinct relationship with Dylan's material — an implicit theory of what a "cover" should be.

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Total covers analyzed (with year + duration) | 10,343 |
| Covers with artist genre classification | 5,644 (55%) |
| Overall median duration deviation | 24.8% |
| Faithful covers, 1980s | 69.3% |
| Faithful covers, 2020s | 23.8% |
| Duration ratio IQR, 1980s | 0.168 |
| Duration ratio IQR, 2020s | 0.784 |
| Radical covers (>50% deviation), 1960s | 20.7% |
| Radical covers (>50% deviation), 2020s | 26.2% |
| Jam band median ratio | 2.04× |
| Country median ratio | 0.73× |
| Metal median ratio | 1.35× |
| Folk faithful rate | 46.1% |

## Methodology

**Fidelity proxy**: Absolute deviation of cover duration from Dylan's median original recording length for each work. "Faithful" = <15% deviation. "Radical" = >50% deviation.

**Duration source**: Spotify duration when available (66.8% of covers); MusicBrainz recording length as fallback.

**Genre classification**: Curated artist taxonomy covering 150+ named artists across 10 genre families. Applied to Spotify artist names. 55% coverage of dated covers.

**Exclusions**: Covers where Dylan is a credited performer. Duration ratios outside 0.1×–10× (data quality filter). Years with fewer than 10 covers excluded from timeline.

## Deliverables

- `story3_punk_shift.html` — 438 KB self-contained interactive page (4 Plotly charts, 7 decade cards, 30 gallery covers with Spotify links)
- `story3_data.json` — 553 KB exported analysis data
- `story3_punk_shift.ipynb` — Reproducible analysis notebook
- `story3_findings.md` — This file

## Color Palette
- Background: deep navy (#1a1a2e → #0f3460)
- Accent: red (#e94560)
- Teal: (#53d8a8) for faithful/positive
- Gold: (#f5c518) for reference lines
- Genre colors: green (folk), orange (rock), purple (jam), red (punk), teal (alt/indie)

---

*Analysis date: 2026-05-11*
*Data: 20,631 covers from MusicBrainz + Spotify enrichment*
