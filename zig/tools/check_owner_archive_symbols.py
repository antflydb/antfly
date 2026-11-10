#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reject raw media codecs and migrated lake builders in consumer archives.

Run against installed, unstripped Debug archives built with both ownership
options. This checks definitions, never undefined references or archive sizes.
The tiny pre-existing chunker PNG encoder is an explicit separate exception.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

ZIG = Path(__file__).resolve().parents[1]
ARCHIVES = (
    "antfly-inference-core",
    "antfly-runtime-api_kernel",
    "antfly-runtime-cli",
    "antfly-runtime-distributed",
    "antfly-runtime-enrichment_compute",
    "antfly-runtime-inference",
    "antfly-runtime-local_query",
    "antfly-runtime-serverless",
    "antfly-storage-engine",
    "antfly-storage-kernel",
)
MEDIA = "antfly-runtime-enrichment_compute"
LAKE = "antfly-runtime-serverless"
CHUNKER = frozenset(
    (
        "png.appendChunk",
        "png.encodeRgba",
        "png.encodeZlibNoCompression",
        "png.encodedRgbaSize",
    )
)
RAW = re.compile(r"^(?:(?:image|pdf)\.)?(bmp|gif|jpeg|webp|jpeg2000|jbig2|ccitt|png)\.")
PDF = re.compile(r"^(?:reader\.Reader\.|render\.)")
LAKE_BUILD = re.compile(
    r"lake_execution_build_impl\.|lake_index_publication_builders\.|lake_index_native_(?:text|sparse|dense|rows)\.build(?:Incremental)?\b|lake_index_native_aggregates\.build(?:Indexed|Incremental|WithReuse)?\b"
)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def definitions(output):
    result = set()
    for line in output.splitlines():
        match = re.match(r"^\s*(?:([0-9a-fA-F]+)\s+)?([A-Za-z?])\s+(.+?)\s*$", line)
        if not match:
            continue
        address, kind, name = match.groups()
        if kind == "U" or (address is None and kind in ("w", "v")):
            continue
        # Mach-O prepends one underscore to defined linker symbols.
        if name.startswith("_"):
            name = name[1:]
        result.add(name)
    return result


def endpoints():
    expected = set()
    transport = (
        ZIG / "pkg/antfly-embedded/src/storage/media_native_abi.zig"
    ).read_text()
    image_version = re.search(r'prefix \+\+ name \+\+ "_v(\d+)"', transport).group(1)
    for family in ("image", "pdf"):
        source = (
            ZIG / ("pkg/antfly-embedded/src/storage/" + family + "_native_contract.zig")
        ).read_text()
        methods = re.search(
            r"pub const Methods = struct \{(.*?)\n\};", source, re.S
        ).group(1)
        names = re.findall(r"^\s*([A-Za-z_][A-Za-z_0-9]*): \*const fn", methods, re.M)
        version = (
            image_version
            if family == "image"
            else re.search(r'name \+\+ "_v(\d+)"', source).group(1)
        )
        if not names:
            raise RuntimeError("missing operation methods for " + family)
        expected.update(
            "antfly_" + family + "_native_" + name + "_v" + version for name in names
        )
    return expected


def classify(symbols):
    return {
        "raw_media": sorted(
            value
            for value in symbols
            if (RAW.search(value) and value not in CHUNKER) or PDF.search(value)
        ),
        "chunker_png": sorted(symbols & CHUNKER),
        "lake_builders": sorted(value for value in symbols if LAKE_BUILD.search(value)),
        "qualification_io": sorted(
            value
            for value in symbols
            if "runtime_io_abi.dispatchSwitchForQualification" in value
        ),
        "endpoints": sorted(
            value
            for value in symbols
            if value.startswith(("antfly_image_native_", "antfly_pdf_native_"))
        ),
    }


def audit(inventories, expected):
    errors = []
    for owner, inventory in inventories.items():
        if inventory.get("qualification_io"):
            errors.append(owner + " defines qualification-only alternate IO dispatch")
        if owner != MEDIA and inventory["raw_media"]:
            errors.append(owner + " defines raw media implementations")
        if owner != MEDIA and inventory["endpoints"]:
            errors.append(owner + " defines media owner endpoints")
        if owner != LAKE and inventory["lake_builders"]:
            errors.append(owner + " defines migrated lake builders")
    media = inventories.get(MEDIA, {})
    missing = expected - set(media.get("endpoints", []))
    if missing:
        errors.append("media owner missing endpoints: " + ", ".join(sorted(missing)))
    families = {
        RAW.search(value).group(1)
        for value in media.get("raw_media", [])
        if RAW.search(value)
    }
    if families != {"bmp", "gif", "jpeg", "webp", "jpeg2000", "jbig2", "ccitt", "png"}:
        errors.append(
            "media codec definition families incomplete: " + repr(sorted(families))
        )
    if not any(PDF.search(value) for value in media.get("raw_media", [])):
        errors.append("media owner missing PDF implementation definitions")
    if not inventories.get(LAKE, {}).get("lake_builders"):
        errors.append("serverless owner missing migrated lake builders")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-dir", type=Path, default=ZIG / "zig-out/lib")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = {
        "format_version": 1,
        "scope": "defined Debug archive symbols; no installed binary size claim",
        "passed": False,
        "archives": {},
        "errors": [],
    }
    try:
        for owner in ARCHIVES:
            path = args.archive_dir / ("lib" + owner + ".a")
            if not path.is_file():
                raise RuntimeError("missing required installed archive: " + str(path))
            output = subprocess.run(
                ["nm", "-a", str(path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            ).stdout
            inventory = classify(definitions(output))
            inventory.update(
                path=str(path), sha256=digest(path), bytes=path.stat().st_size
            )
            report["archives"][owner] = inventory
        report["expected_endpoints"] = sorted(endpoints())
        report["errors"] = audit(report["archives"], set(report["expected_endpoints"]))
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        report["errors"].append(str(error))
    report["passed"] = not report["errors"]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "archives": len(report["archives"]),
                "errors": report["errors"],
            }
        )
    )
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
