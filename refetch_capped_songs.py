"""Selective re-fetch script for capped songs only.

This script re-fetches ONLY the 208 songs that were capped at 100 covers
in the previous iteration, without touching the correctly fetched songs.
"""

from __future__ import annotations

import argparse
import csv
import logging
import time
from pathlib import Path
from typing import List, Dict

import pandas as pd

from musicbrainz_parser import MusicBrainzParser, ParserConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_capped_songs(csv_path: Path) -> List[Dict[str, str]]:
    """Load the list of capped songs to re-fetch."""
    capped_songs = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            capped_songs.append(row)
    logger.info(f"Loaded {len(capped_songs)} capped songs to re-fetch")
    return capped_songs


def refetch_single_work(
    parser: MusicBrainzParser,
    work_id: str,
    work_title: str,
    old_count: int
) -> pd.DataFrame:
    """Re-fetch recordings for a single work."""
    logger.info(f"Re-fetching: {work_title} (had {old_count} covers, expecting >>100)")

    # Create a minimal work DataFrame for this single work
    work_df = pd.DataFrame([{
        'work_id': work_id,
        'title': work_title,
    }])

    # Fetch recordings using the existing parser
    recordings_df = parser.fetch_recordings(work_df)

    new_count = len(recordings_df)
    logger.info(f"  ✅ Fetched {new_count} recordings (was {old_count}, gain: +{new_count - int(old_count)})")

    if new_count <= int(old_count) + 10:
        logger.warning(f"  ⚠️  Count didn't increase much - may still be capped or song has few covers")

    return recordings_df


def main():
    parser_arg = argparse.ArgumentParser(
        description="Re-fetch only capped songs from previous iteration"
    )
    parser_arg.add_argument(
        "--capped-list",
        default="capped_songs_to_refetch.csv",
        help="CSV file with capped songs list"
    )
    parser_arg.add_argument(
        "--output-dir",
        default="data",
        help="Directory to save re-fetched recordings"
    )
    parser_arg.add_argument(
        "--db-url",
        default=None,
        help="MusicBrainz database URL (optional)"
    )
    parser_arg.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of songs to re-fetch (for testing)"
    )
    parser_arg.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Start from song number N (for resuming)"
    )

    args = parser_arg.parse_args()

    # Load capped songs list
    capped_songs = load_capped_songs(Path(args.capped_list))

    # Apply limit and start_from
    if args.start_from > 0:
        logger.info(f"Starting from song #{args.start_from}")
        capped_songs = capped_songs[args.start_from:]

    if args.limit:
        logger.info(f"Limiting to {args.limit} songs for testing")
        capped_songs = capped_songs[:args.limit]

    # Initialize parser
    config = ParserConfig(
        artist_name="Bob Dylan",
        data_dir=Path(args.output_dir),
        sleep_seconds=1.1  # Respect MusicBrainz rate limit
    )
    parser = MusicBrainzParser(parser_config=config, db_url=args.db_url)

    # Re-fetch each capped song
    all_refetched = []
    stats = {
        'total': len(capped_songs),
        'completed': 0,
        'failed': 0,
        'total_old_covers': 0,
        'total_new_covers': 0,
    }

    for i, song in enumerate(capped_songs, start=args.start_from + 1):
        logger.info(f"\n[{i}/{args.start_from + len(capped_songs)}] Processing: {song['title']}")

        try:
            recordings_df = refetch_single_work(
                parser,
                song['work_id'],
                song['title'],
                song['old_count']
            )

            all_refetched.append(recordings_df)
            stats['completed'] += 1
            stats['total_old_covers'] += int(song['old_count'])
            stats['total_new_covers'] += len(recordings_df)

        except Exception as e:
            logger.error(f"  ❌ Failed to re-fetch {song['title']}: {e}")
            stats['failed'] += 1
            continue

        # Save checkpoint every 10 songs
        if stats['completed'] % 10 == 0:
            logger.info(f"\n💾 Checkpoint: {stats['completed']} songs completed")
            if all_refetched:
                checkpoint_df = pd.concat(all_refetched, ignore_index=True)
                checkpoint_path = Path(args.output_dir) / f"refetched_recordings_checkpoint_{stats['completed']}.csv"
                checkpoint_df.to_csv(checkpoint_path, index=False)
                logger.info(f"   Saved checkpoint to {checkpoint_path}")

    # Combine all refetched recordings
    if all_refetched:
        final_df = pd.concat(all_refetched, ignore_index=True)
        output_path = Path(args.output_dir) / "refetched_recordings.csv"
        final_df.to_csv(output_path, index=False)
        logger.info(f"\n✅ Saved all re-fetched recordings to {output_path}")
        logger.info(f"   Total recordings: {len(final_df)}")

    # Print summary stats
    logger.info("\n" + "=" * 60)
    logger.info("RE-FETCH SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Songs processed: {stats['completed']}/{stats['total']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info(f"Old total covers: {stats['total_old_covers']}")
    logger.info(f"New total covers: {stats['total_new_covers']}")
    logger.info(f"Gain: +{stats['total_new_covers'] - stats['total_old_covers']} covers")
    logger.info(f"Improvement: {(stats['total_new_covers'] / stats['total_old_covers'] - 1) * 100:.1f}% increase")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
