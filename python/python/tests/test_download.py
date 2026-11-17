# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import io
import tarfile

import pytest
from lance.download import _safe_tar_extractall


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "parent", "fifo", "device"])
def test_reject_unsafe_archive(tmp_path, kind):
    dest = tmp_path / "dest"
    dest.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    member = tarfile.TarInfo("shortcut")
    if kind == "symlink":
        member.type = tarfile.SYMTYPE
        member.linkname = "../outside"
    elif kind == "hardlink":
        member.type = tarfile.LNKTYPE
        member.linkname = "../outside/marker"
    elif kind == "parent":
        member.name = "../outside/marker"
    elif kind == "fifo":
        member.type = tarfile.FIFOTYPE
    else:
        member.type = tarfile.CHRTYPE
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        archive.addfile(member)
        archive.addfile(tarfile.TarInfo("shortcut/marker"))
    payload.seek(0)
    with tarfile.open(fileobj=payload) as archive:
        with pytest.raises((tarfile.FilterError, RuntimeError)):
            _safe_tar_extractall(archive, dest)
    assert list(outside.iterdir()) == []


def test_safe_archive_and_missing_filter(tmp_path, monkeypatch):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        member = tarfile.TarInfo("nested/dictionary")
        member.size = 4
        archive.addfile(member, io.BytesIO(b"data"))
    payload.seek(0)
    with tarfile.open(fileobj=payload) as archive:
        _safe_tar_extractall(archive, tmp_path)
        assert (tmp_path / "nested/dictionary").read_bytes() == b"data"
        monkeypatch.delattr(tarfile, "data_filter")
        with pytest.raises(RuntimeError, match="Safe tar extraction"):
            _safe_tar_extractall(archive, tmp_path)


def test_preexisting_escaping_link(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (dest / "shortcut").symlink_to(outside, target_is_directory=True)
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        archive.addfile(tarfile.TarInfo("shortcut/marker"))
    payload.seek(0)
    with tarfile.open(fileobj=payload) as archive:
        with pytest.raises(tarfile.FilterError):
            _safe_tar_extractall(archive, dest)
    assert list(outside.iterdir()) == []
