#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
"""
One-time setup: fetch the JOB/IMDb CSV dump into `benchmarks/datasets/job/.data`.
The archive must match `DATA_SHA256`. Its `schematext.sql` is not extracted: the
committed `job/schematext.sql` is the schema, so no SQL from the archive ever runs.
The data is IMDb's, for non-commercial research only; see benchmarks/datasets/README.md.
Run from the repository root:

    python -m benchmarks.datasets.job.setup.download_job_data
"""

import hashlib
import tarfile
import urllib.request
from pathlib import Path

from benchmarks.datasets.job.job_dataset import JOB_ARCHIVE_NAME, JOB_FULL_DATA_DIR

DATA_URL = "https://event.cwi.nl/da/job/imdb.tgz"
DATA_SHA256 = "25f9d893c54f903366e0c263f88db0d429dbc2b159d4987ebc1e203242a7e988"
# the size of that archive: a server that sends more is cut off instead of filling
# the disk before the checksum could reject it
DATA_SIZE = 1_263_193_115
DOWNLOAD_TIMEOUT = 60  # seconds without data before the download gives up
SCHEMA_FILE_NAME = "schematext.sql"


class ChecksumMismatch(Exception):
    """An archive whose SHA-256 is not `DATA_SHA256`."""


class DownloadTooLarge(Exception):
    """A download that goes on past `DATA_SIZE` bytes."""


class IncompleteDownload(Exception):
    """A download that ended before `DATA_SIZE` bytes arrived."""


def main() -> None:
    """Download, verify and extract the JOB dump into `JOB_FULL_DATA_DIR`."""
    JOB_FULL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    archive = JOB_FULL_DATA_DIR / JOB_ARCHIVE_NAME

    if archive.exists():
        _check_checksum(archive)
    else:
        # an interrupted download leaves only the .part file, which the next run
        # overwrites, so a truncated archive is never taken for a complete one
        partial = archive.with_name(archive.name + ".part")
        print(f"downloading {DATA_URL} -> {archive}")
        _download(partial)
        _check_checksum(partial)
        partial.rename(archive)

    print(f"extracting {archive} ...")
    with tarfile.open(archive) as tar:
        # a generator, not a list: listing all members first would decompress the
        # whole archive twice
        data_members = (
            member for member in tar if Path(member.name).name != SCHEMA_FILE_NAME
        )
        tar.extractall(JOB_FULL_DATA_DIR, members=data_members, filter="data")
    archive.unlink()

    tables = list(JOB_FULL_DATA_DIR.glob("*.csv"))
    print(f"done: {len(tables)} tables in {JOB_FULL_DATA_DIR}")


def _download(path: Path) -> None:
    """Write `DATA_URL` to `path`. Raises `DownloadTooLarge` once more than
    `DATA_SIZE` bytes arrive, and `IncompleteDownload` when fewer arrived: a
    connection that closes early ends the read without an error."""
    received = 0
    with urllib.request.urlopen(DATA_URL, timeout=DOWNLOAD_TIMEOUT) as response:
        with path.open("wb") as file:
            while chunk := response.read(1 << 20):  # 1 MiB chunks
                received += len(chunk)
                if received > DATA_SIZE:
                    raise DownloadTooLarge(f"{DATA_URL} sent more than {DATA_SIZE} B")
                file.write(chunk)

    if received != DATA_SIZE:
        raise IncompleteDownload(f"received {received} of {DATA_SIZE} B; run again")


def _check_checksum(path: Path) -> None:
    """Raise `ChecksumMismatch` unless the SHA-256 of `path` is `DATA_SHA256`."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1 << 20):  # 1 MiB chunks
            digest.update(chunk)

    actual = digest.hexdigest()
    if actual != DATA_SHA256:
        raise ChecksumMismatch(
            f"{path} has SHA-256 {actual}, expected {DATA_SHA256};"
            " delete it and run again"
        )


if __name__ == "__main__":
    main()
