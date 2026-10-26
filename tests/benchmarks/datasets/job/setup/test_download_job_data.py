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

"""The one-time JOB download, run without the network."""

import hashlib
import io
import tarfile
from typing import NoReturn

import pytest

from benchmarks.datasets.job.setup import download_job_data

ONE_CSV = {"title.csv": b"1,Nocturne\n"}  # a tiny stand-in for the dump


def _archive_with(members: dict[str, bytes]) -> bytes:
    """A tiny stand-in for imdb.tgz holding the given files. Build it once per test:
    gzip stores the current second, so two builds can differ."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, content in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def _trust(monkeypatch, archive: bytes) -> None:
    """Make `archive` the one archive that the download accepts."""
    digest = hashlib.sha256(archive).hexdigest()
    monkeypatch.setattr(download_job_data, "DATA_SHA256", digest)


def _serve(monkeypatch, response) -> list[tuple[str, float]]:
    """Make `response` what the server sends; the returned list collects the URL and
    the timeout of every request."""
    requests = []

    def urlopen(url: str, timeout: float):
        """The fake server: remembers the request and answers with `response`."""
        requests.append((url, timeout))
        return response

    monkeypatch.setattr(download_job_data.urllib.request, "urlopen", urlopen)
    return requests


def _no_server(monkeypatch) -> None:
    """Fail the test if the download is attempted at all."""

    def urlopen(url: str, timeout: float) -> NoReturn:
        """The fake server that must not be asked."""
        raise AssertionError(f"no download expected, but {url} was requested")

    monkeypatch.setattr(download_job_data.urllib.request, "urlopen", urlopen)


class _EndlessResponse:
    """A response that never ends: every read returns 16 more bytes. After
    `MAX_READS` reads it fails the test, in case the download does not stop it."""

    MAX_READS = 1000

    def __init__(self) -> None:
        """No reads yet."""
        self.reads = 0

    def __enter__(self) -> "_EndlessResponse":
        """Used as `with urlopen(...) as response`."""
        return self

    def __exit__(self, *exc_info) -> None:
        """Nothing to close."""

    def read(self, size: int = -1) -> bytes:
        """16 more bytes, whatever `size` asks for."""
        self.reads += 1
        assert self.reads <= self.MAX_READS, "the download was not cut off"
        return b"x" * 16


class _DroppedConnection(io.BytesIO):
    """A response that delivers some bytes and then loses the connection."""

    def read(self, size: int = -1) -> bytes:
        """At most 4 bytes on the first call; every later call raises."""
        if self.tell() > 0:
            raise ConnectionResetError("connection lost mid-download")
        return super().read(4)


def test_main_interrupted_download_leaves_no_archive(tmp_path, monkeypatch):
    """A broken download leaves only the .part file, never a truncated imdb.tgz."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    archive = _archive_with(ONE_CSV)  # the expected archive
    _trust(monkeypatch, archive)  # the checksum is not the failure
    _serve(monkeypatch, _DroppedConnection(archive))  # breaks after 4 bytes

    with pytest.raises(ConnectionResetError):  # the run fails visibly
        download_job_data.main()  # the one run that breaks off mid-download

    assert not (tmp_path / "imdb.tgz").exists()  # nothing claims to be complete
    assert (tmp_path / "imdb.tgz.part").exists()  # only the partial file remains


def test_main_existing_archive_extracts_without_download(tmp_path, monkeypatch):
    """A complete archive from an earlier run is extracted, not fetched again."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    archive = _archive_with(ONE_CSV)  # the expected archive
    (tmp_path / "imdb.tgz").write_bytes(archive)  # already there
    _trust(monkeypatch, archive)  # the archive is the expected one
    _no_server(monkeypatch)  # a download would fail the test

    download_job_data.main()  # extract, delete the archive

    assert (tmp_path / "title.csv").read_bytes() == b"1,Nocturne\n"  # extracted
    assert not (tmp_path / "imdb.tgz").exists()  # the archive is removed after use


def test_main_after_interruption_downloads_and_extracts(tmp_path, monkeypatch):
    """The next run overwrites the leftover .part, then downloads and extracts."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    (tmp_path / "imdb.tgz.part").write_bytes(b"trunc")  # left by a broken run
    archive = _archive_with(ONE_CSV)  # this time the server delivers
    _trust(monkeypatch, archive)  # the server sends the expected one
    monkeypatch.setattr(download_job_data, "DATA_SIZE", len(archive))  # exactly
    requests = _serve(monkeypatch, io.BytesIO(archive))  # the fake server

    download_job_data.main()  # download, rename, extract, delete the archive

    assert requests == [(download_job_data.DATA_URL, 60)]  # one request, 60 s
    assert (tmp_path / "title.csv").read_bytes() == b"1,Nocturne\n"  # extracted
    assert not (tmp_path / "imdb.tgz").exists()  # the archive is removed after use
    assert not (tmp_path / "imdb.tgz.part").exists()  # renamed, not left behind


def test_main_download_beyond_DATA_SIZE_raises_DownloadTooLarge(tmp_path, monkeypatch):
    """A server that never stops sending is cut off once the archive's size is
    passed, so it cannot fill the disk before the checksum would reject the file."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    monkeypatch.setattr(download_job_data, "DATA_SIZE", 100)  # a tiny archive
    _serve(monkeypatch, _EndlessResponse())  # 16 bytes per read, forever

    with pytest.raises(download_job_data.DownloadTooLarge):  # stops while reading
        download_job_data.main()  # the download overruns the limit

    written = (tmp_path / "imdb.tgz.part").stat().st_size  # what reached the disk
    assert written <= 100  # never more than the archive's size
    assert not (tmp_path / "imdb.tgz").exists()  # never accepted as the archive


def test_main_download_shorter_than_DATA_SIZE_raises_IncompleteDownload(
    tmp_path, monkeypatch
):
    """A connection that closes early ends the read without an error; the download
    then says so, instead of reporting a checksum mismatch."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    archive = _archive_with(ONE_CSV)  # what the server manages to send
    monkeypatch.setattr(download_job_data, "DATA_SIZE", len(archive) + 1)  # 1 B short
    _serve(monkeypatch, io.BytesIO(archive))  # the fake server closes early

    incomplete = download_job_data.IncompleteDownload  # not a checksum mismatch
    with pytest.raises(incomplete, match="run again"):  # says what to do
        download_job_data.main()  # the download ends one byte short

    assert not (tmp_path / "imdb.tgz").exists()  # never accepted as the archive


def test_DATA_URL_uses_https():
    """The archive is fetched over an encrypted connection."""
    assert download_job_data.DATA_URL.startswith("https://")  # no plain HTTP


def test_main_swapped_download_is_rejected_and_not_kept(tmp_path, monkeypatch):
    """An archive with another checksum raises and is never renamed to imdb.tgz."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    _trust(monkeypatch, _archive_with(ONE_CSV))  # the expected archive
    swapped = _archive_with({"title.csv": b"9,Swapped\n"})  # other bytes
    monkeypatch.setattr(download_job_data, "DATA_SIZE", len(swapped))  # full size
    _serve(monkeypatch, io.BytesIO(swapped))  # a man in the middle answers

    with pytest.raises(download_job_data.ChecksumMismatch):  # the run fails visibly
        download_job_data.main()  # download, check, stop

    assert not (tmp_path / "imdb.tgz").exists()  # the bad file is never accepted
    assert not (tmp_path / "title.csv").exists()  # and nothing is extracted


def test_main_existing_archive_with_wrong_checksum_raises(tmp_path, monkeypatch):
    """An archive left by an earlier run is checked too, and not extracted if bad."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    _trust(monkeypatch, _archive_with(ONE_CSV))  # the expected archive
    swapped = _archive_with({"title.csv": b"9,Swapped\n"})  # other bytes
    (tmp_path / "imdb.tgz").write_bytes(swapped)  # left by an earlier run
    _no_server(monkeypatch)  # a download would fail the test

    mismatch = download_job_data.ChecksumMismatch  # the archive is not the one
    with pytest.raises(mismatch, match="delete it"):  # says what to do
        download_job_data.main()  # check, stop, and say what to do

    assert not (tmp_path / "title.csv").exists()  # nothing is extracted


def test_main_never_extracts_the_schema_of_the_archive(tmp_path, monkeypatch):
    """No schematext.sql of the archive is extracted, at the top or deeper down, so
    no SQL from the archive can ever run."""
    monkeypatch.setattr(download_job_data, "JOB_FULL_DATA_DIR", tmp_path)  # no .data
    archive = _archive_with(
        {
            "title.csv": b"1,Nocturne\n",
            "schematext.sql": b"COPY x TO 'evil';",
            "sub/schematext.sql": b"COPY x TO 'evil';",
        }
    )  # the dump with hostile schemas
    (tmp_path / "imdb.tgz").write_bytes(archive)  # already there
    _trust(monkeypatch, archive)  # the checksum matches the hostile archive
    _no_server(monkeypatch)  # a download would fail the test

    download_job_data.main()  # check, extract

    assert (tmp_path / "title.csv").read_bytes() == b"1,Nocturne\n"  # data extracted
    assert not (tmp_path / "schematext.sql").exists()  # the schema is left out
    assert not (tmp_path / "sub" / "schematext.sql").exists()  # also deeper down
