"""Regenerate all 5 story HTML artifacts from corrected dataset.

Reads data/dylan_covers_with_popularity.csv (10,444 deduplicated covers)
and data/dylan_recordings.csv (for Dylan's original durations), then
updates the inline DATA JSON objects in each HTML artifact.
"""

import json
import re
import numpy as np
import pandas as pd
from pathlib import Path

ARTIFACTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "artifacts"
DATA_DIR = Path("data")


def load_data():
    covers = pd.read_csv(DATA_DIR / "dylan_covers_with_popularity.csv")
    recordings = pd.read_csv(DATA_DIR / "dylan_recordings.csv")
    dylan_recs = recordings[recordings["is_bob_dylan"] == True].copy()
    return covers, dylan_recs


def patch_html(filepath, new_data_json, old_total=None, new_total=None):
    """Replace the inline DATA JSON in an HTML file."""
    text = filepath.read_text(encoding="utf-8")

    # Replace const DATA = {...};
    pattern = r'(const DATA\s*=\s*)(\{.*?\});'
    match = re.search(pattern, text, re.DOTALL)
    if not match:
        raise ValueError(f"Could not find DATA object in {filepath}")

    text = text[:match.start(2)] + new_data_json + text[match.end(2):]

    # Replace old total counts in footer/hero if provided
    if old_total and new_total:
        text = text.replace(str(old_total), str(new_total))

    filepath.write_text(text, encoding="utf-8")
    print(f"  Updated {filepath.name}")


# =====================================================================
# STORY 2: All Along the Watchtower
# =====================================================================
def generate_story2(covers, dylan_recs):
    print("\nStory 2: All Along the Watchtower")
    wt = covers[covers["work_title"].str.contains("All Along the Watchtower", case=False, na=False)].copy()
    total = len(wt)
    print(f"  {total} watchtower covers")

    # Dylan's original duration — use median of recordings > 1 minute (skip fragments)
    dylan_wt = dylan_recs[dylan_recs["work_title"].str.contains("All Along the Watchtower", case=False, na=False)]
    valid_wt = dylan_wt[dylan_wt["recording_length_ms"] > 60000]["recording_length_ms"].dropna()
    original_ms = valid_wt.median() if len(valid_wt) > 0 else 152000  # fallback: 2:32
    original_min = round(original_ms / 60000, 2)

    # Spotify-matched subset
    sp = wt[wt["spotify_track_id"].notna()].copy()
    sp["duration_min"] = sp["spotify_duration_ms"].apply(lambda x: round(x / 60000, 2) if pd.notna(x) else None)

    # Extract year from spotify_release_date
    def extract_year(d):
        if pd.isna(d):
            return None
        s = str(d)
        if len(s) >= 4:
            try:
                return int(s[:4])
            except ValueError:
                return None
        return None

    sp["year"] = sp["spotify_release_date"].apply(extract_year)

    # Timeline
    timeline_df = sp[sp["year"].notna()].groupby("year").size().reset_index(name="count")
    timeline = [{"year": int(r["year"]), "count": int(r["count"])} for _, r in timeline_df.iterrows()]

    # Duration scatter
    dur_sp = sp[sp["duration_min"].notna() & sp["spotify_popularity"].notna()].copy()
    duration_scatter = []
    for _, r in dur_sp.iterrows():
        duration_scatter.append({
            "artist": str(r["spotify_artist_name"]),
            "year": int(r["year"]) if pd.notna(r.get("year")) else None,
            "duration": float(r["duration_min"]),
            "popularity": int(r["spotify_popularity"]),
            "track_name": str(r["spotify_track_name"]),
        })

    # Duration histogram
    durations = dur_sp["duration_min"].dropna().values
    if len(durations) > 0:
        counts, edges = np.histogram(durations, bins=30)
        duration_hist = {
            "counts": counts.tolist(),
            "edges": edges.tolist(),
        }
        median_dur = float(np.median(durations))
    else:
        duration_hist = {"counts": [], "edges": []}
        median_dur = 0

    # Fidelity / popularity vs deviation
    pop_dev = []
    for _, r in dur_sp.iterrows():
        dev = abs(float(r["duration_min"]) - original_min)
        pop_dev.append({
            "artist": str(r["spotify_artist_name"]),
            "popularity": int(r["spotify_popularity"]),
            "deviation": round(dev, 2),
            "duration": float(r["duration_min"]),
        })

    # Canonical covers (top 20 by popularity)
    top = sp.nlargest(20, "spotify_popularity")
    canonical = []
    for _, r in top.iterrows():
        canonical.append({
            "artist": str(r["spotify_artist_name"]),
            "popularity": int(r["spotify_popularity"]),
            "year": int(r["year"]) if pd.notna(r.get("year")) else None,
            "duration_min": float(r["duration_min"]) if pd.notna(r.get("duration_min")) else None,
            "album": str(r["spotify_album_name"]) if pd.notna(r.get("spotify_album_name")) else "",
            "spotify_url": str(r["spotify_external_url"]) if pd.notna(r.get("spotify_external_url")) else None,
            "track_name": str(r["spotify_track_name"]),
        })

    # Year range
    years_with_data = sp["year"].dropna()
    year_min = int(years_with_data.min()) if len(years_with_data) > 0 else 1968
    year_max = int(years_with_data.max()) if len(years_with_data) > 0 else 2025

    # Peak year
    if len(timeline) > 0:
        peak = max(timeline, key=lambda x: x["count"])
        peak_year = peak["year"]
        peak_count = peak["count"]
    else:
        peak_year = 2007
        peak_count = 0

    stats = {
        "total_recordings": total,
        "unique_spotify_tracks": len(sp),
        "year_range": [year_min, year_max],
        "duration_range": [round(float(durations.min()), 1), round(float(durations.max()), 1)] if len(durations) > 0 else [0, 0],
        "duration_median": round(median_dur, 1),
        "original_duration_min": original_min,
        "hendrix_year": 1968,
        "peak_year": peak_year,
        "peak_year_count": peak_count,
    }

    data = {
        "timeline": timeline,
        "duration_hist": duration_hist,
        "duration_scatter": duration_scatter,
        "canonical_covers": canonical,
        "stats": stats,
        "popularity_vs_deviation": pop_dev,
    }

    # Update HTML
    filepath = ARTIFACTS_DIR / "story2_watchtower.html"
    data_json = json.dumps(data, ensure_ascii=False)
    html = filepath.read_text(encoding="utf-8")

    # Replace DATA
    pattern = r'(const DATA\s*=\s*)(\{.*?\});'
    match = re.search(pattern, html, re.DOTALL)
    if match:
        html = html[:match.start(2)] + data_json + html[match.end(2):]

    # Update hero stat
    html = re.sub(r'The Song That Changed <em>\d+ Times</em>',
                  f'The Song That Changed <em>{total} Times</em>', html)
    html = re.sub(r'Then \d+ more artists tried',
                  f'Then {total - 2} more artists tried', html)

    # Update stat cards
    html = re.sub(r'(<div class="value">)\d+(</div>\s*<div class="desc">Unique Recordings)',
                  f'\\g<1>{total}\\2', html)
    html = re.sub(r'(\d+\.\d+) min(</div>\s*<div class="desc">Median Duration)',
                  f'{median_dur} min\\2', html)

    # Update year span
    html = re.sub(r'1968&ndash;\d{4}', f'1968&ndash;{year_max}', html)

    # Update body text mentions of 558
    html = html.replace("558 recordings", f"{total} recordings")
    html = html.replace("558 unique recordings", f"{total} unique recordings")
    html = html.replace("Each of its 558", f"Each of its {total}")
    html = html.replace("1967&ndash;2025", f"1967&ndash;{year_max}")

    # Update "Among 456 recordings"
    dur_count = len(durations)
    html = re.sub(r'Among \d+ recordings', f'Among {dur_count} recordings', html)

    filepath.write_text(html, encoding="utf-8")
    print(f"  Updated story2_watchtower.html ({total} covers, {len(sp)} Spotify)")


# =====================================================================
# STORY 3: Punk / Fidelity Shift
# =====================================================================
def generate_story3(covers, dylan_recs):
    print("\nStory 3: How Punk Killed the Reverential Cover")

    # Get Dylan's original duration per work — use median of recordings > 1 minute
    # (min() picks tiny fragment recordings, e.g. 10-second snippets)
    valid_dylan = dylan_recs[dylan_recs["recording_length_ms"] > 60000]
    dylan_dur = valid_dylan.groupby("work_title")["recording_length_ms"].median().dropna()

    # Merge covers with original durations
    df = covers.copy()
    df["original_ms"] = df["work_title"].map(dylan_dur)

    # Use Spotify duration if available, otherwise recording_length_ms
    df["cover_ms"] = df["spotify_duration_ms"].fillna(df["recording_length_ms"])

    # Filter to covers with both durations
    df = df[df["original_ms"].notna() & df["cover_ms"].notna()].copy()
    df["duration_ratio"] = df["cover_ms"] / df["original_ms"]

    # Extract year
    def extract_year(row):
        for col in ["spotify_release_date", "first_release_date"]:
            val = row.get(col)
            if pd.notna(val):
                s = str(val)
                if len(s) >= 4:
                    try:
                        return int(s[:4])
                    except ValueError:
                        pass
        return None

    df["year"] = df.apply(extract_year, axis=1)
    dated = df[df["year"].notna() & (df["year"] >= 1960) & (df["year"] <= 2026)].copy()

    total_analyzed = len(dated)
    print(f"  {total_analyzed} covers with duration + date data")

    # Faithful: within 15% of original
    dated["is_faithful"] = (dated["duration_ratio"] >= 0.85) & (dated["duration_ratio"] <= 1.15)
    dated["is_radical"] = (dated["duration_ratio"] < 0.5) | (dated["duration_ratio"] > 1.5)

    # Fidelity timeline (per year)
    fidelity_timeline = []
    for year, group in dated.groupby("year"):
        ratios = group["duration_ratio"]
        fidelity_timeline.append({
            "year": int(year),
            "n": len(group),
            "median_ratio": float(ratios.median()),
            "q25": float(ratios.quantile(0.25)),
            "q75": float(ratios.quantile(0.75)),
            "pct_faithful": float(group["is_faithful"].mean()),
            "pct_radical": float(group["is_radical"].mean()),
        })

    # Decade breakdown — HTML expects numeric decade keys and pct as fractions
    dated["decade"] = (dated["year"] // 10 * 10).astype(int)
    valid_decades = {1960, 1970, 1980, 1990, 2000, 2010, 2020}

    decade_fidelity = []
    for decade, group in dated.groupby("decade"):
        if decade not in valid_decades:
            continue
        ratios = group["duration_ratio"]
        decade_fidelity.append({
            "decade": int(decade),
            "n": len(group),
            "median_ratio": round(float(ratios.median()), 3),
            "iqr": round(float(ratios.quantile(0.75) - ratios.quantile(0.25)), 3),
            "pct_faithful": round(float(group["is_faithful"].mean()), 4),
            "pct_radical": round(float(group["is_radical"].mean()), 4),
            "pct_moderate": round(float((~group["is_faithful"] & ~group["is_radical"]).mean()), 4),
        })

    # Genre-based analysis (approximate from artist names)
    genre_keywords = {
        "Punk": ["Ramones", "Sex Pistols", "Dead Kennedys", "NOFX", "Me First and the Gimme Gimmes",
                 "Envy on the Coast", "T.S.O.L.", "Pennywise"],
        "Jam Band": ["Grateful Dead", "Dead & Company", "Jerry Garcia", "Phish", "Dave Matthews",
                     "Widespread Panic", "Allman Brothers"],
        "Country": ["Johnny Cash", "Willie Nelson", "Waylon Jennings", "Emmylou Harris",
                    "Darius Rucker", "Old Crow Medicine Show", "Hank Williams"],
        "Folk": ["Joan Baez", "Pete Seeger", "Peter, Paul and Mary", "Judy Collins",
                 "Richie Havens", "Odetta"],
        "Metal": ["Guns N' Roses", "Anthrax", "Iron Maiden", "Metallica"],
        "Alt/Indie": ["Cat Power", "Patti Smith", "Jeff Buckley", "Thea Gilmore",
                      "Walk off the Earth", "Laura Marling"],
        "Jazz": ["Nina Simone", "Diana Krall", "Cassandra Wilson", "Brad Mehldau",
                 "Norah Jones", "Madeleine Peyroux"],
        "Rock": ["Eric Clapton", "Bryan Ferry", "U2", "Pearl Jam", "The Byrds", "Manfred Mann"],
    }

    def classify_genre(artist_name):
        if not isinstance(artist_name, str):
            return "Other"
        for genre, artists in genre_keywords.items():
            for a in artists:
                if a.lower() in artist_name.lower():
                    return genre
        return "Other"

    dated["genre"] = dated["cover_artist_name"].apply(classify_genre)

    genre_spectrum = []
    for genre, group in dated.groupby("genre"):
        if genre == "Other" or len(group) < 5:
            continue
        ratios = group["duration_ratio"]
        genre_spectrum.append({
            "artist_genre": genre,
            "n": len(group),
            "median_ratio": round(float(ratios.median()), 3),
            "q25": round(float(ratios.quantile(0.25)), 3),
            "q75": round(float(ratios.quantile(0.75)), 3),
            "mean_ratio": round(float(ratios.mean()), 3),
            "pct_faithful": round(float(group["is_faithful"].mean()), 4),
        })
    genre_spectrum.sort(key=lambda x: x["median_ratio"])

    # Scatter data — each cover with Spotify data, deduplicated by track ID
    scatter_sp = dated[dated["spotify_track_id"].notna()].copy()
    scatter_sp = scatter_sp.drop_duplicates(subset=["spotify_track_id"])
    scatter_data = []
    for _, r in scatter_sp.iterrows():
        scatter_data.append({
            "year": int(r["year"]),
            "ratio": round(float(r["duration_ratio"]), 3),
            "genre": str(r["genre"]),
            "artist": str(r["spotify_artist_name"]) if pd.notna(r.get("spotify_artist_name")) else str(r["cover_artist_name"]),
            "song": str(r["work_title"]),
            "pop": int(r["spotify_popularity"]) if pd.notna(r.get("spotify_popularity")) else 0,
        })

    # Gallery — split into radical and faithful, deduplicated
    gallery_sp = dated[dated["spotify_track_id"].notna()].copy()
    gallery_sp = gallery_sp.drop_duplicates(subset=["spotify_track_id"])

    def make_gallery_item(r):
        return {
            "artist": str(r["spotify_artist_name"]) if pd.notna(r.get("spotify_artist_name")) else str(r["cover_artist_name"]),
            "song": str(r["work_title"]),
            "year": int(r["year"]),
            "popularity": int(r["spotify_popularity"]) if pd.notna(r.get("spotify_popularity")) else 0,
            "ratio": round(float(r["duration_ratio"]), 3),
            "duration_min": round(float(r["cover_ms"]) / 60000, 2),
            "dylan_duration_min": round(float(r["original_ms"]) / 60000, 2),
            "genre": str(r["genre"]),
            "spotify_url": str(r["spotify_external_url"]) if pd.notna(r.get("spotify_external_url")) else None,
        }

    radical = gallery_sp[gallery_sp["is_radical"]].nlargest(15, "spotify_popularity")
    gallery_radical = [make_gallery_item(r) for _, r in radical.iterrows()]

    faithful = gallery_sp[gallery_sp["is_faithful"]].nlargest(15, "spotify_popularity")
    gallery_faithful = [make_gallery_item(r) for _, r in faithful.iterrows()]

    # Compute hero stats
    faithful_80s = None
    faithful_20s = None
    iqr_80s = None
    iqr_20s = None
    for ds in decade_fidelity:
        if ds["decade"] == 1980:
            faithful_80s = round(ds["pct_faithful"] * 100, 1)
            iqr_80s = ds["iqr"]
        if ds["decade"] == 2020:
            faithful_20s = round(ds["pct_faithful"] * 100, 1)
            iqr_20s = ds["iqr"]

    iqr_ratio = round(iqr_20s / iqr_80s, 1) if iqr_80s and iqr_20s else 4.7

    data = {
        "fidelity_timeline": fidelity_timeline,
        "decade_fidelity": decade_fidelity,
        "genre_spectrum": genre_spectrum,
        "scatter": scatter_data,
        "gallery_radical": gallery_radical,
        "gallery_faithful": gallery_faithful,
        "stats": {
            "total_analyzed": total_analyzed,
            "faithful_80s_pct": faithful_80s,
            "faithful_20s_pct": faithful_20s,
            "iqr_80s": iqr_80s,
            "iqr_20s": iqr_20s,
            "iqr_ratio": iqr_ratio,
        }
    }

    # Update HTML
    filepath = ARTIFACTS_DIR / "story3_punk_shift.html"
    data_json = json.dumps(data, ensure_ascii=False)
    html = filepath.read_text(encoding="utf-8")

    pattern = r'(const DATA\s*=\s*)(\{.*?\});'
    match = re.search(pattern, html, re.DOTALL)
    if match:
        html = html[:match.start(2)] + data_json + html[match.end(2):]

    # Update hero counter
    html = re.sub(r'data-counter="\d+"', f'data-counter="{total_analyzed}"', html)
    # Update faithful % text
    if faithful_80s and faithful_20s:
        html = re.sub(r'\d+%&rarr;\d+%',
                      f'{faithful_80s:.0f}%&rarr;{faithful_20s:.0f}%', html)
    if iqr_ratio:
        html = re.sub(r'\d+\.\d+&times;', f'{iqr_ratio}&times;', html)

    # Update body text IQR values
    if iqr_80s:
        html = re.sub(r'IQR of just \d+\.\d+', f'IQR of just {iqr_80s:.3f}', html)
    if iqr_20s:
        html = re.sub(r'exploded to \d+\.\d+', f'exploded to {iqr_20s:.3f}', html)
    if iqr_80s and iqr_20s:
        html = re.sub(r'Duration ratio IQR: 1980s \(\d+\.\d+\) &rarr; 2020s \(\d+\.\d+\)',
                      f'Duration ratio IQR: 1980s ({iqr_80s:.3f}) &rarr; 2020s ({iqr_20s:.3f})', html)

    filepath.write_text(html, encoding="utf-8")
    print(f"  Updated story3_punk_shift.html ({total_analyzed} covers)")


# =====================================================================
# STORY 4: Protest Song Lifecycle
# =====================================================================
def generate_story4(covers, dylan_recs):
    print("\nStory 4: The Protest Song Lifecycle")

    protest_songs = [
        "Blowin' in the Wind",
        "The Times They Are a-Changin'",
        "Masters of War",
        "A Hard Rain's a-Gonna Fall",
        "With God on Our Side",
    ]

    short_titles = {
        "Blowin' in the Wind": "Blowin' in the Wind",
        "The Times They Are a-Changin'": "The Times They Are A-Changin'",
        "Masters of War": "Masters of War",
        "A Hard Rain's a-Gonna Fall": "A Hard Rain's a-Gonna Fall",
        "With God on Our Side": "With God on Our Side",
    }

    # Match protest songs (fuzzy)
    def is_protest(title):
        if not isinstance(title, str):
            return False, None
        t = title.lower()
        if "blowin" in t and "wind" in t:
            return True, "Blowin' in the Wind"
        if "times they are" in t:
            return True, "The Times They Are A-Changin'"
        if "masters of war" in t:
            return True, "Masters of War"
        if "hard rain" in t:
            return True, "A Hard Rain's a-Gonna Fall"
        if "with god on our side" in t:
            return True, "With God on Our Side"
        return False, None

    protest_rows = []
    for _, row in covers.iterrows():
        is_p, short = is_protest(row["work_title"])
        if is_p:
            r = row.to_dict()
            r["short_title"] = short
            protest_rows.append(r)

    pdf = pd.DataFrame(protest_rows)
    total = len(pdf)
    print(f"  {total} protest song covers")

    # Extract year
    def extract_year(row):
        for col in ["spotify_release_date", "first_release_date"]:
            val = row.get(col)
            if pd.notna(val):
                s = str(val)
                if len(s) >= 4:
                    try:
                        return int(s[:4])
                    except ValueError:
                        pass
        return None

    pdf["year_int"] = pdf.apply(extract_year, axis=1)
    sp = pdf[pdf["spotify_track_id"].notna()].copy()

    # Political eras
    def get_era(year):
        if year is None or pd.isna(year):
            return None
        y = int(year)
        if y < 1963:
            return "Pre-Vietnam"
        if y <= 1975:
            return "Vietnam"
        if y <= 1989:
            return "Punk / Reagan"
        if y <= 2000:
            return "Post-Cold War"
        if y <= 2009:
            return "Post-9/11"
        if y <= 2019:
            return "Occupy / Trump"
        return "BLM / COVID"

    sp["era"] = sp["year_int"].apply(get_era)
    era_order = ["Pre-Vietnam", "Vietnam", "Punk / Reagan", "Post-Cold War",
                 "Post-9/11", "Occupy / Trump", "BLM / COVID"]

    # Timeline (per song per year)
    dated_sp = sp[sp["year_int"].notna()].copy()
    timeline = []
    for (year, song), group in dated_sp.groupby(["year_int", "short_title"]):
        timeline.append({"year_int": int(year), "short_title": song, "count": len(group)})

    # Aggregate timeline
    timeline_agg = []
    for year, group in dated_sp.groupby("year_int"):
        timeline_agg.append({"year": int(year), "count": len(group)})

    # Era × song heatmap
    era_song = []
    for (era, song), group in dated_sp.groupby(["era", "short_title"]):
        if era and era != "Pre-Vietnam":
            era_song.append({"era": era, "short_title": song, "count": len(group)})

    # Heatmap data
    heatmap = []
    for (song, era), group in dated_sp.groupby(["short_title", "era"]):
        if era and era != "Pre-Vietnam":
            heatmap.append({"short_title": song, "era": era, "count": len(group)})

    # Popularity by era
    popularity_by_era = []
    for era, group in dated_sp.groupby("era"):
        if era and era != "Pre-Vietnam":
            pops = group["spotify_popularity"].dropna()
            if len(pops) > 0:
                popularity_by_era.append({
                    "era": era,
                    "mean": round(float(pops.mean()), 1),
                    "median": float(pops.median()),
                    "count": len(group),
                })

    # Song stats
    song_stats = []
    for song in ["Blowin' in the Wind", "The Times They Are A-Changin'",
                 "Masters of War", "A Hard Rain's a-Gonna Fall", "With God on Our Side"]:
        song_df = pdf[pdf["short_title"] == song]
        song_sp = sp[sp["short_title"] == song]
        pops = song_sp["spotify_popularity"].dropna()
        song_stats.append({
            "title": song,
            "total_recordings": len(song_df),
            "spotify_tracks": len(song_sp),
            "median_popularity": float(pops.median()) if len(pops) > 0 else 0,
        })

    # Filter out Spotify false matches (covers matched to Dylan's own track)
    sp_clean = sp[sp["spotify_artist_name"] != "Bob Dylan"].drop_duplicates(subset=["spotify_track_id"])
    dated_sp_clean = dated_sp[dated_sp["spotify_artist_name"] != "Bob Dylan"].drop_duplicates(subset=["spotify_track_id"])

    # Top by era
    top_by_era = {}
    for era, group in dated_sp_clean.groupby("era"):
        if era and era != "Pre-Vietnam":
            top = group.nlargest(5, "spotify_popularity")
            top_by_era[era] = []
            for _, r in top.iterrows():
                top_by_era[era].append({
                    "artist": str(r["spotify_artist_name"]) if pd.notna(r.get("spotify_artist_name")) else str(r["cover_artist_name"]),
                    "song": str(r["short_title"]),
                    "popularity": int(r["spotify_popularity"]),
                    "year": str(r["spotify_release_date"]) if pd.notna(r.get("spotify_release_date")) else str(r.get("first_release_date", "")),
                })

    # Top covers overall
    top_covers = sp_clean.nlargest(20, "spotify_popularity")
    top_list = []
    for _, r in top_covers.iterrows():
        top_list.append({
            "artist": str(r["spotify_artist_name"]) if pd.notna(r.get("spotify_artist_name")) else "",
            "song": str(r["short_title"]),
            "popularity": int(r["spotify_popularity"]),
            "year": str(r["spotify_release_date"]) if pd.notna(r.get("spotify_release_date")) else "",
            "spotify_url": str(r["spotify_external_url"]) if pd.notna(r.get("spotify_external_url")) else None,
            "album": str(r["spotify_album_name"]) if pd.notna(r.get("spotify_album_name")) else "",
        })

    # Year range
    years = dated_sp["year_int"].dropna()
    year_min = int(years.min()) if len(years) > 0 else 1963
    year_max = int(years.max()) if len(years) > 0 else 2025
    year_span = year_max - year_min

    data = {
        "timeline": timeline,
        "timeline_agg": timeline_agg,
        "era_song": era_song,
        "era_order": era_order,
        "heatmap": heatmap,
        "top_by_era": top_by_era,
        "popularity_by_era": popularity_by_era,
        "song_stats": song_stats,
        "top_covers": top_list,
        "stats": {
            "total_recordings": total,
            "total_spotify": len(sp),
            "songs_count": 5,
            "song_titles": ["Blowin' in the Wind", "The Times They Are A-Changin'",
                           "Masters of War", "A Hard Rain's a-Gonna Fall", "With God on Our Side"],
            "year_range": [year_min, year_max],
        },
    }

    filepath = ARTIFACTS_DIR / "story4_protest.html"
    data_json = json.dumps(data, ensure_ascii=False)
    html = filepath.read_text(encoding="utf-8")

    pattern = r'(const DATA\s*=\s*)(\{.*?\});'
    match = re.search(pattern, html, re.DOTALL)
    if match:
        html = html[:match.start(2)] + data_json + html[match.end(2):]

    # Update hero stats
    html = re.sub(r'(<span class="number">)\d+(</span>\s*<span class="label">Cover Recordings)',
                  f'\\g<1>{total}\\2', html)
    html = re.sub(r'(<span class="number">)\d+(</span>\s*<span class="label">Years Covered)',
                  f'\\g<1>{year_span}\\2', html)

    # Update body text
    html = html.replace("762 cover recordings", f"{total} cover recordings")
    html = html.replace("762 recordings", f"{total} recordings")
    html = html.replace("762 unique recordings", f"{total} unique recordings")
    html = html.replace("contains 762", f"contains {total}")
    html = re.sub(r'A <em>\d+-Year</em> Study', f'A <em>{year_span}-Year</em> Study', html)
    html = re.sub(r'1963&ndash;\d{4}', f'1963&ndash;{year_max}', html)

    # Update footer reference to year range
    html = re.sub(r'Sixty-two years', f'{year_span} years', html)

    filepath.write_text(html, encoding="utf-8")
    print(f"  Updated story4_protest.html ({total} covers, {len(sp)} Spotify)")


# =====================================================================
# STORY 5: American Songbook
# =====================================================================
def generate_story5(covers, dylan_recs):
    print("\nStory 5: The American Songbook")

    # Curated songbook artists list
    songbook_artists = {
        "Johnny Cash": "Country Standards",
        "Bryan Ferry": "Art-Pop Crooner",
        "Elvis Presley": "Rock/Pop Standards",
        "Jeff Buckley": "Art Rock",
        "Barb Jungr": "Cabaret",
        "Rod Stewart": "Standards Pop",
        "Cat Power": "Indie Interpretive",
        "Judy Collins": "Folk Standards",
        "James Last": "Orchestral",
        "Richie Havens": "Folk/Soul",
        "Nina Simone": "Jazz Vocal",
        "Willie Nelson": "Country Standards",
        "Adele": "Pop/Soul",
        "Bettye LaVette": "Soul",
        "Odetta": "Folk/Jazz",
        "Melanie": "Folk Pop",
        "Diana Krall": "Jazz Vocal",
        "Norah Jones": "Jazz Vocal",
        "Cassandra Wilson": "Jazz Vocal",
        "Madeleine Peyroux": "Jazz Vocal",
        "Brad Mehldau": "Jazz Piano",
        "Michael Bublé": "Jazz Pop",
    }

    # Match covers to songbook artists
    def match_songbook(artist_name):
        if not isinstance(artist_name, str):
            return None
        a_lower = artist_name.lower()
        for sb_artist in songbook_artists:
            if sb_artist.lower() in a_lower:
                return sb_artist
        return None

    covers["songbook_artist"] = covers["cover_artist_name"].apply(match_songbook)
    sb_covers = covers[covers["songbook_artist"].notna()].copy()
    total_sb = len(sb_covers)
    print(f"  {total_sb} songbook-style covers")

    # Artist stats
    artist_data = []
    for artist, style in songbook_artists.items():
        ac = sb_covers[sb_covers["songbook_artist"] == artist]
        sp = ac[ac["spotify_track_id"].notna()]
        pops = sp["spotify_popularity"].dropna()

        # Get top song
        if len(sp) > 0:
            top_row = sp.nlargest(1, "spotify_popularity").iloc[0]
            top_song = str(top_row["work_title"])
            top_pop = int(top_row["spotify_popularity"])
            spotify_url = str(top_row["spotify_external_url"]) if pd.notna(top_row.get("spotify_external_url")) else None
        else:
            top_song = ""
            top_pop = 0
            spotify_url = None

        # Period (from release dates)
        dates = ac["first_release_date"].dropna().astype(str)
        years = []
        for d in dates:
            if len(d) >= 4:
                try:
                    years.append(int(d[:4]))
                except ValueError:
                    pass

        if years:
            min_decade = f"{min(years) // 10 * 10}s"
            max_decade = f"{max(years) // 10 * 10}s"
            period = f"{min_decade}–{max_decade}" if min_decade != max_decade else min_decade
        else:
            period = ""

        artist_data.append({
            "artist": artist,
            "style": style,
            "period": period,
            "covers": len(ac),
            "unique_songs": ac["work_title"].nunique(),
            "avg_popularity": round(float(pops.mean()), 1) if len(pops) > 0 else 0,
            "max_popularity": int(pops.max()) if len(pops) > 0 else 0,
            "top_song": top_song,
            "top_popularity": top_pop,
            "spotify_url": spotify_url,
        })

    artist_data.sort(key=lambda x: x["covers"], reverse=True)

    # Standard songs (most covered by songbook artists)
    song_sb_counts = sb_covers.groupby("work_title").agg(
        songbook_covers=("recording_id", "count"),
        num_artists=("songbook_artist", "nunique"),
    ).reset_index()

    # Total covers per song
    all_song_counts = covers.groupby("work_title")["recording_id"].count().reset_index(name="all_covers")
    song_sb_counts = song_sb_counts.merge(all_song_counts, on="work_title", how="left")

    # Avg popularity
    sp_sb = sb_covers[sb_covers["spotify_popularity"].notna()]
    song_pops = sp_sb.groupby("work_title")["spotify_popularity"].mean().reset_index(name="avg_pop")
    song_sb_counts = song_sb_counts.merge(song_pops, on="work_title", how="left")
    song_sb_counts["avg_pop"] = song_sb_counts["avg_pop"].fillna(0)
    song_sb_counts["songbook_pct"] = round(song_sb_counts["songbook_covers"] / song_sb_counts["all_covers"] * 100, 1)
    song_sb_counts = song_sb_counts.sort_values("songbook_covers", ascending=False).head(15)

    standard_songs = []
    for _, r in song_sb_counts.iterrows():
        standard_songs.append({
            "song": str(r["work_title"]),
            "songbook_covers": int(r["songbook_covers"]),
            "all_covers": int(r["all_covers"]),
            "num_artists": int(r["num_artists"]),
            "avg_pop": round(float(r["avg_pop"]), 1),
            "songbook_pct": float(r["songbook_pct"]),
        })

    # Timeline by decade
    def get_decade(row):
        for col in ["spotify_release_date", "first_release_date"]:
            val = row.get(col)
            if pd.notna(val):
                s = str(val)
                if len(s) >= 4:
                    try:
                        y = int(s[:4])
                        return f"{y // 10 * 10}s"
                    except ValueError:
                        pass
        return None

    covers["decade"] = covers.apply(get_decade, axis=1)
    sb_covers["decade"] = sb_covers.apply(get_decade, axis=1).values  # Align index

    decade_order = ["1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "2020s"]
    timeline_data = []
    for decade in decade_order:
        all_dec = covers[covers["decade"] == decade]
        sb_dec = sb_covers[sb_covers["decade"] == decade]
        all_count = len(all_dec)
        sb_count = len(sb_dec)
        pct = round(sb_count / all_count * 100, 1) if all_count > 0 else 0
        timeline_data.append({
            "decade": decade,
            "all_covers": all_count,
            "songbook_covers": sb_count,
            "pct": pct,
        })

    # Style breakdown
    style_data = []
    for style, group in sb_covers.groupby(sb_covers["songbook_artist"].map(songbook_artists)):
        pops = group[group["spotify_popularity"].notna()]["spotify_popularity"]
        artist_names = sorted(group["songbook_artist"].unique().tolist())
        style_data.append({
            "style": style,
            "covers": len(group),
            "artists": len(artist_names),
            "artist_names": artist_names,
            "avg_pop": round(float(pops.mean()), 1) if len(pops) > 0 else 0,
        })
    style_data.sort(key=lambda x: x["covers"], reverse=True)

    # Make You Feel My Love breakdown
    myflm = covers[covers["work_title"].str.contains("Make You Feel My Love", case=False, na=False)].copy()
    myflm_by_artist = myflm.groupby("cover_artist_name").agg(
        covers=("recording_id", "count"),
    ).reset_index()
    myflm_sp = myflm[myflm["spotify_popularity"].notna()]
    myflm_pops = myflm_sp.groupby("cover_artist_name")["spotify_popularity"].mean().reset_index(name="avg_pop")
    myflm_by_artist = myflm_by_artist.merge(myflm_pops, on="cover_artist_name", how="left")
    myflm_by_artist["avg_pop"] = myflm_by_artist["avg_pop"].fillna(0)
    myflm_by_artist = myflm_by_artist.sort_values("covers", ascending=False).head(12)

    myflm_data = []
    for _, r in myflm_by_artist.iterrows():
        myflm_data.append({
            "artist": str(r["cover_artist_name"]),
            "covers": int(r["covers"]),
            "avg_pop": round(float(r["avg_pop"]), 1),
        })

    # Total MYFLM covers and Adele's popularity
    myflm_total = len(myflm)
    adele_rows = myflm[myflm["cover_artist_name"].str.contains("Adele", case=False, na=False)]
    adele_pop = int(adele_rows["spotify_popularity"].max()) if len(adele_rows) > 0 else 72

    total_covers = len(covers)
    sb_pct = round(total_sb / total_covers * 100, 1)
    num_sb_artists = len([a for a in artist_data if a["covers"] > 0])

    data = {
        "artists": artist_data,
        "standard_songs": standard_songs,
        "timeline": timeline_data,
        "styles": style_data,
        "make_you_feel_my_love": myflm_data,
    }

    filepath = ARTIFACTS_DIR / "story5_songbook.html"
    data_json = json.dumps(data, ensure_ascii=False)
    html = filepath.read_text(encoding="utf-8")

    pattern = r'(const DATA\s*=\s*)(\{.*?\});'
    match = re.search(pattern, html, re.DOTALL)
    if match:
        html = html[:match.start(2)] + data_json + html[match.end(2):]

    # Update hero stats
    html = re.sub(r"(<span class=\"number\">)\d+(</span>\s*<span class=\"label\">Songbook artists)",
                  f"\\g<1>{num_sb_artists}\\2", html)
    html = re.sub(r"(<span class=\"number\">)[\d,]+(</span>\s*<span class=\"label\">Standards-style covers)",
                  f"\\g<1>{total_sb:,}\\2", html)
    html = re.sub(r"(<span class=\"number\">)[\d.]+%(</span>\s*<span class=\"label\">Of all Dylan covers)",
                  f"\\g<1>{sb_pct}%\\2", html)

    # Update MYFLM stats
    html = re.sub(r"(<span class=\"big\">)\d+(</span>\s*<span class=\"desc\">Covers of)",
                  f"\\g<1>{myflm_total}\\2", html)
    html = re.sub(r"(<span class=\"big\">)\d+(</span>\s*<span class=\"desc\">Adele)",
                  f"\\g<1>{adele_pop}\\2", html)

    # Update footer
    html = html.replace("20,631 covers analyzed", f"{total_covers:,} covers analyzed")

    # Update MYFLM narrative text
    html = re.sub(r"276 artists have", f"{myflm_total} artists have", html)

    filepath.write_text(html, encoding="utf-8")
    print(f"  Updated story5_songbook.html ({total_sb} songbook covers)")


# =====================================================================
# STORY 6: Geography
# =====================================================================
def generate_story6(covers, dylan_recs):
    print("\nStory 6: The Geography of Interpretation")

    # Artist → country mapping (curated)
    # NOTE: Longer/more-specific keys must come before shorter ones
    # (e.g., "George Harrison & Ravi Shankar" before "George Harrison")
    artist_countries = {
        "George Harrison & Ravi Shankar": "India",
        "The Byrds": "United States", "Grateful Dead": "United States",
        "Joan Baez": "United States", "Bruce Springsteen": "United States",
        "Jerry Garcia": "United States", "Johnny Cash": "United States",
        "Jeff Buckley": "United States", "Cat Power": "United States",
        "Eddie Vedder": "United States", "Pearl Jam": "United States",
        "Richie Havens": "United States", "Pete Seeger": "United States",
        "Peter, Paul and Mary": "United States", "Stevie Wonder": "United States",
        "Nina Simone": "United States", "Willie Nelson": "United States",
        "Odetta": "United States", "Sam Cooke": "United States",
        "Judy Collins": "United States", "Patti Smith": "United States",
        "Bettye LaVette": "United States", "Guns N' Roses": "United States",
        "Tom Petty": "United States", "Rod Stewart": "United States",
        "Darius Rucker": "United States", "Old Crow Medicine Show": "United States",
        "Dead & Company": "United States", "Dave Matthews": "United States",
        "Jimi Hendrix": "United States", "Bob Seger": "United States",
        "Tracy Chapman": "United States", "Adele": "United Kingdom",
        "Eric Clapton": "United Kingdom", "Manfred Mann": "United Kingdom",
        "George Harrison": "United Kingdom", "Bryan Ferry": "United Kingdom",
        "Fairport Convention": "United Kingdom", "XTC": "United Kingdom",
        "The Animals": "United Kingdom", "Nick Cave": "Australia",
        "The Seekers": "Australia",
        "U2": "Ireland", "Van Morrison": "Ireland",
        "The Band": "Canada", "Neil Young": "Canada",
        "Diana Krall": "Canada", "Michael Bublé": "Canada",
        "Leonard Cohen": "Canada", "Joni Mitchell": "Canada",
        "Hugues Aufray": "France",
        "Wolfgang Niedecken": "Germany", "James Last": "Germany",
        "Marlene Dietrich": "Germany", "Wolfgang Ambros": "Germany",
        "Nana Mouskouri": "Greece",
        "Dylan.pl": "Poland",
        "Totta": "Sweden", "Mikael Wiehe": "Sweden",
        "Jan Erik Vold": "Norway",
        "Fabrizio De André": "Italy",
        "Jimmy Cliff": "Jamaica",
        # India (via collaborations)
        "Ravi Shankar": "India",
    }

    def classify_country(artist_name):
        if not isinstance(artist_name, str):
            return None
        for key, country in artist_countries.items():
            if key.lower() in artist_name.lower():
                return country
        return None

    covers["country"] = covers["cover_artist_name"].apply(classify_country)
    classified = covers[covers["country"].notna()].copy()
    total_classified = len(classified)
    print(f"  {total_classified} classified covers")

    # Country stats
    countries_data = []
    for country, group in classified.groupby("country"):
        pops = group[group["spotify_popularity"].notna()]["spotify_popularity"]

        # Top artists
        top_artists_df = group.groupby("cover_artist_name")["recording_id"].count().sort_values(ascending=False).head(5)
        top_artists = [{"name": name, "covers": int(count)} for name, count in top_artists_df.items()]

        # Top songs
        top_songs_df = group.groupby("work_title")["recording_id"].count().sort_values(ascending=False).head(5)
        top_songs = [{"song": name, "covers": int(count)} for name, count in top_songs_df.items()]

        # Translation countries
        translation_countries = {"France", "Germany", "Sweden", "Poland", "Italy", "Norway", "Greece", "India"}

        countries_data.append({
            "country": country,
            "covers": len(group),
            "unique_artists": group["cover_artist_name"].nunique(),
            "unique_songs": group["work_title"].nunique(),
            "avg_pop": round(float(pops.mean()), 1) if len(pops) > 0 else 0,
            "top_artists": top_artists,
            "top_songs": top_songs,
            "has_translations": country in translation_countries,
        })

    countries_data.sort(key=lambda x: x["covers"], reverse=True)

    # Augment India with known Indian Dylan interpreters (not in MusicBrainz)
    # Bengali adaptations (Kabir Suman, Anjan Dutt) + Shillong live tradition (Lou Majaw)
    india_entry = next((c for c in countries_data if c["country"] == "India"), None)
    india_curated = [
        {"name": "Lou Majaw", "covers": 1},
        {"name": "Kabir Suman", "covers": 1},
        {"name": "Anjan Dutt", "covers": 1},
    ]
    if india_entry:
        india_entry["covers"] += 3
        india_entry["unique_artists"] += 3
        india_entry["unique_songs"] = max(india_entry["unique_songs"], 3)
        india_entry["top_artists"].extend(india_curated)
        india_entry["has_translations"] = True
    else:
        countries_data.append({
            "country": "India",
            "covers": 3,
            "unique_artists": 3,
            "unique_songs": 3,
            "avg_pop": 0,
            "top_artists": india_curated,
            "top_songs": [{"song": "Blowin' in the Wind", "covers": 3}],
            "has_translations": True,
        })
    countries_data.sort(key=lambda x: x["covers"], reverse=True)

    # Percentages
    us_covers = sum(c["covers"] for c in countries_data if c["country"] == "United States")
    uk_covers = sum(c["covers"] for c in countries_data if c["country"] == "United Kingdom")
    rest = total_classified - us_covers - uk_covers
    us_pct = round(us_covers / total_classified * 100) if total_classified > 0 else 0
    uk_pct = round(uk_covers / total_classified * 100) if total_classified > 0 else 0
    rest_pct = 100 - us_pct - uk_pct

    # Global songs (covered in 5+ countries)
    song_countries = classified.groupby(["work_title", "country"]).agg(
        covers=("recording_id", "count")
    ).reset_index()

    songs_by_country_count = song_countries.groupby("work_title").agg(
        num_countries=("country", "nunique"),
        total_covers=("covers", "sum"),
    ).reset_index()

    global_songs_df = songs_by_country_count[songs_by_country_count["num_countries"] >= 5].sort_values("num_countries", ascending=False)

    global_songs = []
    for _, row in global_songs_df.iterrows():
        song = row["work_title"]
        sc = song_countries[song_countries["work_title"] == song].sort_values("covers", ascending=False)
        countries_list = [{"country": r["country"], "covers": int(r["covers"])} for _, r in sc.iterrows()]
        global_songs.append({
            "song": song,
            "total_covers": int(row["total_covers"]),
            "num_countries": int(row["num_countries"]),
            "countries": countries_list,
        })

    # Unique countries and languages
    num_countries = classified["country"].nunique()

    # Keep the translations data from the original (editorial content)
    # Just read it from the existing HTML
    translations = {
        "France": [
            {"local": "Les temps changent", "english": "The Times They Are A-Changin'", "artist": "Hugues Aufray"},
            {"local": "Comme des pierres qui roulent", "english": "Like a Rolling Stone", "artist": "Hugues Aufray"},
            {"local": "Cauchemar psychomoteur", "english": "Subterranean Homesick Blues", "artist": "Hugues Aufray"},
            {"local": "N'y pense plus, tout est bien", "english": "Don't Think Twice, It's All Right", "artist": "Hugues Aufray"},
            {"local": "La Fille du nord", "english": "Girl From the North Country", "artist": "Hugues Aufray"},
            {"local": "Dans le souffle du vent", "english": "Blowin' in the Wind", "artist": "Hugues Aufray"},
        ],
        "Germany": [
            {"local": "Die Antwort weiß ganz allein der Wind", "english": "Blowin' in the Wind", "artist": "Marlene Dietrich"},
            {"local": "Für immer jung", "english": "Forever Young", "artist": "Wolfgang Niedecken et al."},
            {"local": "Als ob se'n Frau wöhr", "english": "Just Like a Woman", "artist": "Wolfgang Niedecken"},
            {"local": "Allan wia a Stan", "english": "Like a Rolling Stone", "artist": "Wolfgang Ambros"},
        ],
        "Sweden": [
            {"local": "Adjö Angelina", "english": "Farewell Angelina", "artist": "Totta & Wiehe"},
            {"local": "Dagen är kommen", "english": "The Times They Are A-Changin'", "artist": "Totta & Wiehe"},
        ],
        "Poland": [
            {"local": "Czasy nadchodzą nowe", "english": "The Times They Are A-Changin'", "artist": "Dylan.pl"},
        ],
        "Italy": [
            {"local": "Via della Povertà", "english": "Desolation Row", "artist": "Fabrizio De André"},
        ],
        "Norway": [
            {"local": "Damer i regn", "english": "Rainy Day Women", "artist": "Jan Erik Vold"},
        ],
        "India": [
            {"local": "Er Gaan (Bengali)", "english": "Blowin' in the Wind", "artist": "Kabir Suman"},
            {"local": "Blowin' in the Wind (Bengali adaptation)", "english": "Blowin' in the Wind", "artist": "Anjan Dutt"},
            {"local": "Annual Dylan Festival, Shillong", "english": "Multiple songs (live tradition)", "artist": "Lou Majaw"},
        ],
    }

    # The HTML uses separate COUNTRIES, GLOBAL_SONGS, TRANSLATIONS JS variables
    # Let me construct the JS to replace
    filepath = ARTIFACTS_DIR / "story6_geography.html"
    html = filepath.read_text(encoding="utf-8")

    # Replace COUNTRIES array
    countries_json = json.dumps(countries_data, ensure_ascii=False)
    html = re.sub(r'(const COUNTRIES\s*=\s*)(\[.*?\]);', f'\\1{countries_json};', html, flags=re.DOTALL)

    # Replace GLOBAL_SONGS
    global_json = json.dumps(global_songs, ensure_ascii=False)
    html = re.sub(r'(const GLOBAL_SONGS\s*=\s*)(\[.*?\]);', f'\\1{global_json};', html, flags=re.DOTALL)

    # Replace TRANSLATIONS
    trans_json = json.dumps(translations, ensure_ascii=False)
    html = re.sub(r'(const TRANSLATIONS\s*=\s*)(\{.*?\});', f'\\1{trans_json};', html, flags=re.DOTALL)

    # Update hero stats
    html = re.sub(r"(<span class=\"number\">)\d+(</span>\s*<span class=\"label\">Countries)",
                  f"\\g<1>{num_countries}\\2", html)
    html = re.sub(r"(<span class=\"number\">)[\d,]+(</span>\s*<span class=\"label\">Classified covers)",
                  f"\\g<1>{total_classified:,}\\2", html)

    # Update percentage stats
    html = re.sub(r"(<span class=\"big\">)\d+%(</span>\s*<span class=\"desc\">From United States)",
                  f"\\g<1>{us_pct}%\\2", html)
    html = re.sub(r"(<span class=\"big\">)\d+%(</span>\s*<span class=\"desc\">From United Kingdom)",
                  f"\\g<1>{uk_pct}%\\2", html)
    html = re.sub(r"(<span class=\"big\">)\d+%(</span>\s*<span class=\"desc\">Rest of world)",
                  f"\\g<1>{rest_pct}%\\2", html)

    # Update footer
    html = html.replace("20,631 covers analyzed", f"{len(covers):,} covers analyzed")

    # Update depth stats
    france_songs = next((c["unique_songs"] for c in countries_data if c["country"] == "France"), 0)
    germany_songs = next((c["unique_songs"] for c in countries_data if c["country"] == "Germany"), 0)
    sweden_songs = next((c["unique_songs"] for c in countries_data if c["country"] == "Sweden"), 0)

    html = re.sub(r"(<span class=\"big\">)\d+(</span>\s*<span class=\"desc\">Songs covered in France)",
                  f"\\g<1>{france_songs}\\2", html)
    html = re.sub(r"(<span class=\"big\">)\d+(</span>\s*<span class=\"desc\">Songs covered in Germany)",
                  f"\\g<1>{germany_songs}\\2", html)
    html = re.sub(r"(<span class=\"big\">)\d+(</span>\s*<span class=\"desc\">Songs covered in Sweden)",
                  f"\\g<1>{sweden_songs}\\2", html)

    filepath.write_text(html, encoding="utf-8")
    print(f"  Updated story6_geography.html ({total_classified} classified, {num_countries} countries)")


# =====================================================================
# Main
# =====================================================================
def main():
    print("Loading corrected dataset...")
    covers, dylan_recs = load_data()
    print(f"  {len(covers)} covers, {len(dylan_recs)} Dylan recordings")

    generate_story2(covers, dylan_recs)
    generate_story3(covers, dylan_recs)
    generate_story4(covers, dylan_recs)
    generate_story5(covers, dylan_recs)
    generate_story6(covers, dylan_recs)

    print("\nAll artifacts regenerated!")


if __name__ == "__main__":
    main()
