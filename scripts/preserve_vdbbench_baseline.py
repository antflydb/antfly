"""Freeze executed binaries and qualification evidence, never mutable data dirs.

Creates an exclusive archive and a reviewable JSON catalog. Failed arms remain
in receipts but are excluded from qualified summaries. Historical helper bytes
are archived only if their recorded digest still matches: current source must
not masquerade as the source used for an older measurement.
"""

import argparse
import hashlib
import json
import os
import tarfile
import tempfile
from pathlib import Path

from summarize_projection_locality_ab import summarize


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def preserve(root, archive, catalog):
    root = root.resolve(strict=True)
    archive = archive.resolve()
    catalog = catalog.resolve()
    if archive == catalog or archive.exists() or catalog.exists():
        raise FileExistsError("archive and catalog must be distinct new paths")
    if archive.is_relative_to(root) or catalog.is_relative_to(root):
        raise ValueError("outputs must not modify the source evidence directory")
    receipt_path = root / "ab-runs.json"
    receipt_hash = digest(receipt_path)
    receipts = json.loads(receipt_path.read_text())
    if not receipts:
        raise ValueError("empty qualification receipt set")
    expected = {}
    binary_names = set()
    for receipt in receipts:
        binary_names.add(receipt["environment"]["ANTFLY_BIN"])
        for name, checksum in receipt["inputs_sha256"].items():
            if name in expected and expected[name] != checksum:
                raise ValueError(f"input identity differs between arms: {name}")
            expected[name] = checksum
    inputs = []
    for name, checksum in sorted(expected.items()):
        path = Path(name)
        observed = digest(path) if path.is_file() else None
        if name in binary_names and observed != checksum:
            raise ValueError(f"executed binary unavailable or changed: {name}")
        inputs.append(
            {
                "source_path": name,
                "expected_sha256": checksum,
                "observed_sha256": observed,
                "status": (
                    "matched"
                    if observed == checksum
                    else "unavailable_historical_bytes"
                ),
                "binary": name in binary_names,
            }
        )
    if not binary_names.issubset(expected):
        raise ValueError("binary missing from measured input hashes")
    files = [
        (path, f"evidence/{path.name}")
        for path in sorted(root.iterdir())
        if path.is_file()
    ]
    for receipt in receipts:
        arm = Path(receipt["command"][1]).resolve(strict=True)
        if arm.parent != root:
            raise ValueError(f"arm is not an immediate child of source root: {arm}")
        files.extend(
            (path, f"evidence/{arm.name}/{path.name}")
            for path in sorted(arm.iterdir())
            if path.is_file()
        )
        results = arm / "results"
        if results.is_symlink():
            raise ValueError(f"symlink is not immutable evidence: {results}")
        if results.exists():
            files.extend(
                (path, f"evidence/{arm.name}/results/{path.relative_to(results)}")
                for path in sorted(results.rglob("*"))
                if path.is_file()
            )
    for item in inputs:
        if item["status"] == "matched":
            path = Path(item["source_path"])
            item["archive_path"] = f"inputs/{item['expected_sha256']}/{path.name}"
            files.append((path, item["archive_path"]))
    files = list({name: path for path, name in files}.items())
    fingerprints = {}
    for name, path in files:
        if path.is_symlink():
            raise ValueError(f"symlink is not immutable evidence: {path}")
        fingerprints[name] = {
            "path": name,
            "sha256": digest(path),
            "bytes": path.stat().st_size,
        }
    archive.parent.mkdir(parents=True, exist_ok=True)
    catalog.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "schema_version": 1,
        "source_root": str(root),
        "scope": "executed binaries and evidence; excludes runtime data, models and source checkout",
        "historical_input_warning": "Unmatched historical helpers cannot be reconstructed from current files.",
        "receipts_sha256": receipt_hash,
        "receipts": receipts,
        "qualified_results": summarize(root),
        "inputs": inputs,
        "files": list(fingerprints.values()),
    }
    # Build privately, then publish without replacing any existing baseline.
    with tempfile.TemporaryDirectory(
        prefix="baseline-", dir=archive.parent
    ) as temporary:
        staged = Path(temporary) / "baseline.tar.gz"
        with tarfile.open(staged, "w:gz") as output:
            for name, path in files:
                checksum = fingerprints[name]["sha256"]
                if digest(path) != checksum:
                    raise ValueError(f"evidence changed while summarizing: {path}")
                if str(path) in expected and checksum != expected[str(path)]:
                    raise ValueError(f"measured input changed while freezing: {path}")
                output.add(path, arcname=name, recursive=False)
                if digest(path) != checksum:
                    raise ValueError(f"evidence changed while freezing: {path}")
        # Also reject changes after a file was archived, including mid-summary edits.
        for name, path in files:
            checksum = fingerprints[name]["sha256"]
            if digest(path) != checksum:
                raise ValueError(f"evidence changed during baseline capture: {path}")
        if digest(receipt_path) != receipt_hash:
            raise ValueError("receipts changed during baseline capture")
        record["archive"] = {
            "path": os.path.relpath(archive, catalog.parent),
            "sha256": digest(staged),
            "bytes": staged.stat().st_size,
        }
        os.link(staged, archive)
        # A failure here leaves a recoverable archive, never a valid-looking catalog.
        with tempfile.TemporaryDirectory(
            prefix="baseline-catalog-", dir=catalog.parent
        ) as catalog_temporary:
            staged_catalog = Path(catalog_temporary) / "catalog.json"
            staged_catalog.write_text(json.dumps(record, indent=2) + "\n")
            os.link(staged_catalog, catalog)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    args = parser.parse_args()
    record = preserve(args.root, args.archive, args.catalog)
    print(
        json.dumps(
            {
                "archive": record["archive"],
                "files": len(record["files"]),
                "arms": len(record["receipts"]),
            }
        )
    )


if __name__ == "__main__":
    main()
