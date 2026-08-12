#!/usr/bin/env python3
"""Regression tests for reproducible Antfly release archives."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from typing import List


PACKAGING_DIR = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


reproducible_tar = load_module(
    "create_reproducible_tar_for_test",
    PACKAGING_DIR / "create_reproducible_tar.py",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ReproducibleTarTests(unittest.TestCase):
    def test_archive_is_identical_after_metadata_perturbation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "stage"
            (source / "share" / "antfly").mkdir(parents=True)
            (source / "include").mkdir()
            (source / "README.md").write_text("Antfly\n", encoding="utf-8")
            (source / "share" / "antfly" / "asset.txt").write_text("asset\n", encoding="utf-8")
            (source / "include" / "antfly.h").write_text("/* Antfly */\n", encoding="utf-8")
            binary = source / "antfly"
            binary.write_bytes(b"#!/bin/sh\nexit 0\n")
            binary.chmod(0o700)

            first = root / "first.tar.gz"
            second = root / "second.tar.gz"
            reproducible_tar.create_archive(source, first, 1_700_000_000)

            for index, path in enumerate([source, *source.rglob("*")]):
                os.utime(path, (1_800_000_000 + index, 1_800_000_000 + index))
            binary.chmod(0o755)
            (source / "README.md").chmod(0o600)
            reproducible_tar.create_archive(source, second, 1_700_000_000)

            self.assertEqual(sha256(first), sha256(second))
            self.assertEqual(first.read_bytes(), second.read_bytes())

            gzip_header = first.read_bytes()[:10]
            self.assertEqual(gzip_header[:3], b"\x1f\x8b\x08")
            self.assertEqual(gzip_header[3] & 0x08, 0, "gzip header must not contain a filename")
            self.assertEqual(gzip_header[4:8], b"\0\0\0\0")

            listing = subprocess.check_output(["tar", "-tzf", str(first)], text=True).splitlines()
            self.assertIn("./include/antfly.h", listing)

            with tarfile.open(first, "r:gz") as archive:
                members = archive.getmembers()
            expected_names: List[str] = ["."] + [
                f"./{path.relative_to(source).as_posix()}"
                for path in sorted(source.rglob("*"), key=lambda item: item.relative_to(source).as_posix())
            ]
            self.assertEqual([member.name for member in members], expected_names)
            for member in members:
                self.assertEqual((member.uid, member.gid), (0, 0))
                self.assertEqual((member.uname, member.gname), ("", ""))
                self.assertEqual(member.mtime, 1_700_000_000)
                expected_mode = 0o755 if member.isdir() or member.name == "./antfly" else 0o644
                self.assertEqual(stat.S_IMODE(member.mode), expected_mode)

    def test_rejects_links_and_special_files(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "stage"
            source.mkdir()
            regular = source / "regular"
            regular.write_text("data", encoding="utf-8")

            link = source / "link"
            link.symlink_to(regular.name)
            with self.assertRaisesRegex(ValueError, "symlink"):
                reproducible_tar.create_archive(source, root / "link.tar.gz", 0)
            link.unlink()

            fifo = source / "fifo"
            os.mkfifo(fifo)
            with self.assertRaisesRegex(ValueError, "special file"):
                reproducible_tar.create_archive(source, root / "fifo.tar.gz", 0)


if __name__ == "__main__":
    unittest.main()
