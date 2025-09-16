"""Command line entry point for the Dylan cover analysis pipeline."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

try:  # ``python-dotenv`` is optional.
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None  # type: ignore

from musicbrainz_downloader import PostgresConfig, download_and_prepare
from musicbrainz_parser import MusicBrainzParser, ParserConfig
from spotify_enricher import SpotifyConfig, SpotifyEnricher


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bob Dylan song and cover analysis pipeline",
    )

    parser.add_argument("--data-dir", default="data", help="Directory for CSV exports")
    parser.add_argument("--artist", default="Bob Dylan", help="Artist name to analyse")
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    refresh = parser.add_argument_group("Database refresh")
    refresh.add_argument("--refresh-db", action="store_true", help="Download and import MusicBrainz dump")
    refresh.add_argument("--skip-verify", action="store_true", help="Skip checksum verification when downloading dumps")
    refresh.add_argument("--overwrite-downloads", action="store_true", help="Re-download dump archives even if they exist")
    refresh.add_argument("--use-docker", dest="use_docker", action="store_true", help="Run PostgreSQL inside Docker (default)")
    refresh.add_argument("--no-docker", dest="use_docker", action="store_false", help="Use a locally installed PostgreSQL instance")
    refresh.set_defaults(use_docker=True)
    refresh.add_argument("--db-host", default="localhost")
    refresh.add_argument("--db-port", default=5432, type=int)
    refresh.add_argument("--db-name", default="musicbrainz")
    refresh.add_argument("--db-user", default="musicbrainz")
    refresh.add_argument("--db-password", default="musicbrainz")

    mb_parser = parser.add_argument_group("MusicBrainz parsing")
    mb_parser.add_argument("--get-covers", action="store_true", help="Extract works, recordings and covers from MusicBrainz")
    mb_parser.add_argument("--db-url", default=None, help="SQLAlchemy database URL for MusicBrainz")

    spotify = parser.add_argument_group("Spotify enrichment")
    spotify.add_argument("--enrich-spotify", action="store_true", help="Fetch Spotify popularity for cover tracks")
    spotify.add_argument("--spotify-client-id", default=None)
    spotify.add_argument("--spotify-client-secret", default=None)
    spotify.add_argument("--spotify-redirect-uri", default=None)
    spotify.add_argument("--spotify-market", default="US")

    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def resolve_spotify_config(args: argparse.Namespace, data_dir: Path) -> SpotifyConfig:
    client_id = args.spotify_client_id or os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = args.spotify_client_secret or os.getenv("SPOTIFY_CLIENT_SECRET")
    redirect_uri = args.spotify_redirect_uri or os.getenv("SPOTIFY_REDIRECT_URI")

    if not client_id or not client_secret:
        raise RuntimeError("Spotify client id/secret must be provided via CLI or environment variables")

    return SpotifyConfig(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        market=args.spotify_market,
        data_dir=data_dir,
    )


def main(argv: Optional[list[str]] = None) -> None:
    if load_dotenv is not None:
        load_dotenv()

    parser = build_argument_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.refresh_db:
        postgres_config = PostgresConfig(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password,
            database=args.db_name,
        )
        download_and_prepare(
            verify=not args.skip_verify,
            overwrite=args.overwrite_downloads,
            use_docker=args.use_docker,
            postgres_config=postgres_config,
        )

    if args.get_covers:
        parser_config = ParserConfig(artist_name=args.artist, data_dir=data_dir)
        mb_parser = MusicBrainzParser(parser_config=parser_config, db_url=args.db_url)
        mb_parser.run()

    if args.enrich_spotify:
        spotify_config = resolve_spotify_config(args, data_dir)
        enricher = SpotifyEnricher(spotify_config)
        enricher.enrich()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()

