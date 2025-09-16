"""Utilities for downloading and loading the MusicBrainz database dumps.

This module focuses on the heavy lifting required before we can extract the
discography data for Bob Dylan.  The MusicBrainz project publishes regular
database exports that can be imported into a local PostgreSQL instance.  The
data is fairly large, so the functions here emphasise incremental downloads,
checksum verification and optional Docker based database provisioning.

The overall flow that :mod:`musicbrainz_downloader` supports is:

1. Discover the latest full export on the official FTP mirror.
2. Download the necessary ``.tar.bz2`` archives together with their ``.md5``
   checksums and verify the integrity of the files.
3. Optionally extract the archives and execute the SQL scripts against a
   PostgreSQL database.  Both local installations and Docker based setups are
   supported.

The implementation deliberately avoids any external dependencies except
``requests`` so that the script can run in constrained environments.  Every
operation logs progress which makes it easier to diagnose long running
imports.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import requests


logger = logging.getLogger(__name__)


# The MusicBrainz team asks API users to identify themselves.  We follow the
# same rule of thumb for dump downloads.
DEFAULT_USER_AGENT = "Dylan-Cover-Analysis/1.0 (https://github.com/)"


@dataclass
class PostgresConfig:
    """Configuration container for connecting to PostgreSQL.

    Attributes
    ----------
    host:
        Database host.  ``localhost`` is assumed for the Docker setup.
    port:
        PostgreSQL port.  Defaults to ``5432``.
    user / password:
        Credentials used during ``psql`` invocations.
    database:
        Database name that will host the MusicBrainz schema.
    """

    host: str = "localhost"
    port: int = 5432
    user: str = "musicbrainz"
    password: str = "musicbrainz"
    database: str = "musicbrainz"

    def as_psql_env(self) -> dict[str, str]:
        """Return environment variables suitable for ``psql`` invocations."""

        env = os.environ.copy()
        env.update(
            {
                "PGHOST": self.host,
                "PGPORT": str(self.port),
                "PGUSER": self.user,
                "PGPASSWORD": self.password,
                "PGDATABASE": self.database,
            }
        )
        return env


@dataclass
class DumpConfig:
    """Parameters defining which MusicBrainz dump we want to consume."""

    base_url: str = "https://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport"
    release: Optional[str] = None
    files: List[str] = field(
        default_factory=lambda: [
            "mbdump.tar.bz2",
            "mbdump-derived.tar.bz2",
            "mbdump-stats.tar.bz2",
        ]
    )


class MusicBrainzDownloader:
    """Download, verify and import MusicBrainz database dumps."""

    def __init__(
        self,
        dump_config: Optional[DumpConfig] = None,
        data_dir: Path | str = Path("data/musicbrainz"),
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.dump_config = dump_config or DumpConfig()
        self.data_dir = Path(data_dir)
        self.user_agent = user_agent
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Discovery helpers
    # ------------------------------------------------------------------
    def resolve_release(self) -> str:
        """Resolve the dump release to use.

        MusicBrainz publishes a ``LATEST`` file in every export directory.  If
        the :class:`DumpConfig` did not explicitly specify a release we pull the
        latest one and cache it on the instance.
        """

        if self.dump_config.release:
            return self.dump_config.release

        latest_url = f"{self.dump_config.base_url}/LATEST"
        logger.info("Fetching latest MusicBrainz dump release from %s", latest_url)
        response = self.session.get(latest_url, timeout=30)
        response.raise_for_status()
        release = response.text.strip()
        if not release:
            raise RuntimeError("Unable to determine the latest MusicBrainz release")

        self.dump_config.release = release
        logger.info("Latest dump release resolved to %s", release)
        return release

    # ------------------------------------------------------------------
    # Download logic
    # ------------------------------------------------------------------
    def download_dump(self, verify: bool = True, overwrite: bool = False) -> List[Path]:
        """Download the configured dump files.

        Parameters
        ----------
        verify:
            Whether to validate downloads against the provided ``.md5`` files.
        overwrite:
            Re-download even if files already exist locally.
        """

        release = self.resolve_release()
        release_dir = self.data_dir / release
        release_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files: List[Path] = []
        for file_name in self.dump_config.files:
            target_file = release_dir / file_name
            if target_file.exists() and not overwrite:
                logger.info("File %s already present - skipping download", target_file)
                downloaded_files.append(target_file)
                continue

            file_url = f"{self.dump_config.base_url}/{release}/{file_name}"
            logger.info("Downloading %s", file_url)
            with self.session.get(file_url, stream=True, timeout=120) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            tmp_file.write(chunk)
                    temp_path = Path(tmp_file.name)

            shutil.move(str(temp_path), target_file)
            logger.info("Saved %s", target_file)

            if verify:
                self.verify_checksum(target_file)

            downloaded_files.append(target_file)

        return downloaded_files

    def verify_checksum(self, dump_file: Path) -> None:
        """Verify a downloaded archive against its ``.md5`` checksum."""

        release = self.resolve_release()
        checksum_url = (
            f"{self.dump_config.base_url}/{release}/{dump_file.name}.md5"
        )
        logger.info("Verifying checksum for %s", dump_file)
        response = self.session.get(checksum_url, timeout=30)
        response.raise_for_status()
        expected_checksum = response.text.strip().split()[0]

        hasher = hashlib.md5()
        with dump_file.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        actual_checksum = hasher.hexdigest()

        if actual_checksum != expected_checksum:
            raise RuntimeError(
                f"Checksum mismatch for {dump_file}: {actual_checksum} != {expected_checksum}"
            )
        logger.info("Checksum verified for %s", dump_file)

    # ------------------------------------------------------------------
    # Extraction & import helpers
    # ------------------------------------------------------------------
    def extract_dump(self, dump_file: Path, destination: Optional[Path] = None) -> Path:
        """Extract a ``.tar.bz2`` archive into *destination* and return the path."""

        if destination is None:
            destination = dump_file.with_suffix("")

        logger.info("Extracting %s -> %s", dump_file, destination)
        destination.mkdir(parents=True, exist_ok=True)

        with tarfile.open(dump_file, mode="r:bz2") as archive:
            members = archive.getmembers()
            for member in members:
                member_path = destination / member.name
                try:
                    member_path.relative_to(destination)
                except ValueError as exc:  # pragma: no cover - safety guard
                    raise RuntimeError(
                        f"Unsafe path detected while extracting {dump_file}: {member.name}"
                    ) from exc

            archive.extractall(destination, members)

        logger.info("Extraction complete for %s", dump_file)
        return destination

    def import_sql_files(
        self,
        sql_directory: Path,
        postgres_config: PostgresConfig,
        skip_existing: bool = True,
    ) -> None:
        """Execute the extracted MusicBrainz SQL files using ``psql``.

        The dump archives contain multiple SQL files that have to be executed in
        alphabetical order.  The process may take a long time; therefore we log
        progress and allow resuming by skipping files that already have a
        ``.done`` marker.
        """

        sql_files = sorted(sql_directory.glob("*.sql"))
        if not sql_files:
            raise FileNotFoundError(
                f"No SQL files found in {sql_directory}. Did you extract the dump?"
            )

        for sql_file in sql_files:
            marker = sql_file.with_suffix(sql_file.suffix + ".done")
            if skip_existing and marker.exists():
                logger.info("Skipping %s (marker present)", sql_file)
                continue

            logger.info("Importing %s", sql_file)
            with sql_file.open("rb") as handle:
                result = subprocess.run(
                    ["psql", "-v", "ON_ERROR_STOP=1"],
                    env=postgres_config.as_psql_env(),
                    stdin=handle,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=False,
                )

            if result.returncode != 0:
                logger.error(result.stdout.decode("utf-8", errors="ignore"))
                raise RuntimeError(f"psql import failed for {sql_file}")

            marker.touch()
            logger.info("Finished importing %s", sql_file)

    # ------------------------------------------------------------------
    # Database provisioning
    # ------------------------------------------------------------------
    def ensure_postgres_database(
        self,
        postgres_config: PostgresConfig,
        use_docker: bool = True,
        docker_image: str = "postgres:14",
        container_name: str = "musicbrainz-postgres",
    ) -> None:
        """Ensure that PostgreSQL is available for the MusicBrainz import."""

        if use_docker:
            self._ensure_docker_database(
                postgres_config, docker_image=docker_image, container_name=container_name
            )
        else:
            self._ensure_local_database(postgres_config)

    def _ensure_docker_database(
        self,
        postgres_config: PostgresConfig,
        docker_image: str,
        container_name: str,
    ) -> None:
        """Start a PostgreSQL container if it is not already running."""

        logger.info("Ensuring PostgreSQL Docker container '%s' is running", container_name)
        env_vars = {
            "POSTGRES_USER": postgres_config.user,
            "POSTGRES_PASSWORD": postgres_config.password,
            "POSTGRES_DB": postgres_config.database,
        }

        inspect = subprocess.run(
            ["docker", "inspect", container_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if inspect.returncode == 0:
            logger.info("Docker container '%s' already exists", container_name)
            return

        command = [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "-e",
            f"POSTGRES_USER={postgres_config.user}",
            "-e",
            f"POSTGRES_PASSWORD={postgres_config.password}",
            "-e",
            f"POSTGRES_DB={postgres_config.database}",
            "-p",
            f"{postgres_config.port}:5432",
            docker_image,
        ]
        logger.info("Starting PostgreSQL Docker container: %s", " ".join(command))
        subprocess.run(command, check=True)

    def _ensure_local_database(self, postgres_config: PostgresConfig) -> None:
        """Check if the target database exists and create it if necessary."""

        logger.info(
            "Ensuring local PostgreSQL database '%s' exists", postgres_config.database
        )
        env = postgres_config.as_psql_env()
        env.pop("PGDATABASE")

        # ``psql -lqt`` lists available databases.  We parse the output to see if
        # our target database is present.
        result = subprocess.run(
            ["psql", "-lqt"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            logger.error(result.stderr.decode("utf-8", errors="ignore"))
            raise RuntimeError("Failed to list PostgreSQL databases")

        databases = {
            line.split("|")[0].strip()
            for line in result.stdout.decode("utf-8", errors="ignore").splitlines()
            if line.strip()
        }
        if postgres_config.database in databases:
            logger.info("Database '%s' already exists", postgres_config.database)
            return

        logger.info("Creating database '%s'", postgres_config.database)
        create_result = subprocess.run(
            ["createdb", postgres_config.database],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if create_result.returncode != 0:
            logger.error(create_result.stdout.decode("utf-8", errors="ignore"))
            raise RuntimeError(
                f"Failed to create database {postgres_config.database}. "
                "Ensure the PostgreSQL service is running and the user has permissions."
            )
        logger.info("Database '%s' created", postgres_config.database)


def download_and_prepare(
    verify: bool = True,
    overwrite: bool = False,
    use_docker: bool = True,
    postgres_config: Optional[PostgresConfig] = None,
    dump_config: Optional[DumpConfig] = None,
) -> None:
    """Convenience wrapper used by :mod:`main` to setup MusicBrainz dumps."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    downloader = MusicBrainzDownloader(dump_config=dump_config)
    postgres_config = postgres_config or PostgresConfig()

    downloader.ensure_postgres_database(postgres_config, use_docker=use_docker)
    archives = downloader.download_dump(verify=verify, overwrite=overwrite)

    for archive in archives:
        if not archive.name.startswith("mbdump"):
            logger.info("Skipping extraction/import for %s", archive)
            continue
        extracted = downloader.extract_dump(archive)
        downloader.import_sql_files(extracted, postgres_config)


__all__ = [
    "DumpConfig",
    "PostgresConfig",
    "MusicBrainzDownloader",
    "download_and_prepare",
]

