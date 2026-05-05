"""Merge old (non-capped) data with newly re-fetched capped songs.

Strategy:
1. Load old data from previous_iterations/dylan_recordings_output.csv
2. Load newly re-fetched data from data/refetched_recordings.csv
3. Remove old capped songs (100 covers) from old data
4. Merge old (non-capped) + new (re-fetched) data
5. Identify covers (non-Dylan recordings)
6. Export final merged dataset
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_old_data(path: Path) -> pd.DataFrame:
    """Load previous iteration data."""
    logger.info(f"Loading old data from {path}")
    df = pd.read_csv(path)
    logger.info(f"  Loaded {len(df)} old recordings")
    return df


def load_new_data(path: Path) -> pd.DataFrame:
    """Load newly re-fetched data."""
    logger.info(f"Loading new re-fetched data from {path}")
    df = pd.read_csv(path)
    logger.info(f"  Loaded {len(df)} new recordings")
    return df


def load_capped_work_ids(path: Path) -> set:
    """Load set of work IDs that were capped."""
    logger.info(f"Loading capped work IDs from {path}")
    df = pd.read_csv(path)
    work_ids = set(df['work_id'].unique())
    logger.info(f"  Found {len(work_ids)} unique capped work IDs")
    return work_ids


def merge_datasets(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    capped_work_ids: set
) -> pd.DataFrame:
    """Merge old and new datasets, removing capped songs from old data."""

    # Remove capped songs from old data
    logger.info(f"\nRemoving capped songs from old data...")
    old_non_capped = old_df[~old_df['work_id'].isin(capped_work_ids)].copy()
    logger.info(f"  Kept {len(old_non_capped)} non-capped recordings from old data")
    logger.info(f"  Removed {len(old_df) - len(old_non_capped)} capped recordings")

    # Combine old (non-capped) + new (re-fetched)
    logger.info(f"\nMerging datasets...")
    merged_df = pd.concat([old_non_capped, new_df], ignore_index=True)
    logger.info(f"  Total merged recordings: {len(merged_df)}")

    # Sort by work_title for easier analysis
    merged_df = merged_df.sort_values(['work_title', 'artist_names'])

    return merged_df


def identify_covers(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to only cover versions (non-Dylan recordings)."""
    logger.info(f"\nIdentifying covers (non-Dylan recordings)...")

    # Check if is_bob_dylan column exists
    if 'is_bob_dylan' in df.columns:
        covers_df = df[df['is_bob_dylan'] == False].copy()
    else:
        # Fallback: filter by artist names not containing "Bob Dylan"
        logger.warning("is_bob_dylan column not found, using artist_names filter")
        covers_df = df[~df['artist_names'].str.contains('Bob Dylan', case=False, na=False)].copy()

    logger.info(f"  Found {len(covers_df)} cover recordings")
    logger.info(f"  Excluded {len(df) - len(covers_df)} Dylan's own recordings")

    return covers_df


def print_statistics(old_df: pd.DataFrame, merged_df: pd.DataFrame, covers_df: pd.DataFrame):
    """Print comparison statistics."""
    logger.info("\n" + "=" * 70)
    logger.info("MERGE STATISTICS")
    logger.info("=" * 70)
    logger.info(f"Old dataset (capped):         {len(old_df):,} recordings")
    logger.info(f"New merged dataset:           {len(merged_df):,} recordings")
    logger.info(f"  Gain:                       +{len(merged_df) - len(old_df):,} recordings")
    logger.info(f"  Percentage increase:        +{((len(merged_df) / len(old_df) - 1) * 100):.1f}%")
    logger.info(f"\nCover recordings only:        {len(covers_df):,} covers")
    logger.info("=" * 70)

    # Show top covered songs
    logger.info("\nTOP 20 MOST COVERED SONGS (after merge):")
    cover_counts = covers_df.groupby('work_title').size().sort_values(ascending=False).head(20)
    for i, (song, count) in enumerate(cover_counts.items(), 1):
        logger.info(f"  {i:2}. {song[:50]:<50} {count:>4} covers")


def main():
    parser = argparse.ArgumentParser(
        description="Merge old and new datasets"
    )
    parser.add_argument(
        "--old-data",
        default="previous_iterations/dylan_recordings_output.csv",
        help="Path to old (capped) recordings data"
    )
    parser.add_argument(
        "--new-data",
        default="data/refetched_recordings.csv",
        help="Path to newly re-fetched recordings"
    )
    parser.add_argument(
        "--capped-list",
        default="capped_songs_to_refetch.csv",
        help="CSV with list of capped work IDs"
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory for merged data"
    )

    args = parser.parse_args()

    # Load datasets
    old_df = load_old_data(Path(args.old_data))
    new_df = load_new_data(Path(args.new_data))
    capped_work_ids = load_capped_work_ids(Path(args.capped_list))

    # Merge datasets
    merged_df = merge_datasets(old_df, new_df, capped_work_ids)

    # Identify covers
    covers_df = identify_covers(merged_df)

    # Print statistics
    print_statistics(old_df, merged_df, covers_df)

    # Save outputs
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    recordings_path = output_dir / "dylan_recordings_merged.csv"
    covers_path = output_dir / "dylan_covers_merged.csv"

    merged_df.to_csv(recordings_path, index=False)
    covers_df.to_csv(covers_path, index=False)

    logger.info(f"\n✅ Saved merged recordings to: {recordings_path}")
    logger.info(f"✅ Saved cover recordings to: {covers_path}")


if __name__ == "__main__":
    main()
