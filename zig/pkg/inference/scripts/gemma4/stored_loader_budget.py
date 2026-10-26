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

"""Validate compiled loader-budget evidence without loading model tensors.

The native probe owns the private Zig entry size. This module checks its
arithmetic and geometry; it does not establish that the probe was executed.
Callers must separately bind the probe's binary, build and resource evidence.
"""

import hashlib
import json
from pathlib import Path
import struct

SCHEMA = "antfly.context.stored-loader-budget-preflight.v1"
COORDINATE_FIELDS = (
    "owner_layer", "head", "span", "logical_start", "logical_count",
    "retained_rows", "head_dim",
)
GEOMETRY_DOMAIN = b"antfly.context.stored-loader-geometry.v1\0"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def uint(value, label, bits=64, positive=False):
    require(type(value) is int and int(positive) <= value < 1 << bits, label)
    return value


def digest(value, label):
    require(type(value) is list and len(value) == 32, label)
    for item in value:
        uint(item, label, 8)
    return bytes(value)


def coordinate(value):
    require(type(value) is dict and set(value) == set(COORDINATE_FIELDS), "coordinate fields")
    for name in COORDINATE_FIELDS:
        uint(value[name], name, 32, name in ("logical_count", "retained_rows", "head_dim"))
    require(value["retained_rows"] < value["logical_count"], "coordinate is not lossy")
    require(value["logical_start"] + value["logical_count"] < 1 << 32, "coordinate end overflow")
    return value


def manifest_coordinates(manifest):
    artifacts = manifest.get("artifacts")
    require(type(artifacts) is list and len(artifacts) == 36, "all36 artifacts required")
    require(all(type(row) is dict for row in artifacts), "artifact records")
    coordinates = [coordinate(row["coordinate"]) for row in artifacts]
    order = [(row["owner_layer"], row["head"], row["span"]) for row in coordinates]
    require(all(a < b for a, b in zip(order, order[1:])), "coordinates not strictly ordered")
    reserved = manifest.get("reservation", {}).get("coordinates")
    require(type(reserved) is list and len(reserved) == 36, "reservation coordinates")
    require([coordinate(row) for row in reserved] == coordinates, "reservation coordinates differ")
    for artifact, row in zip(artifacts, coordinates):
        sizes = {"keys": row["retained_rows"] * row["head_dim"] * 2,
                 "values": row["retained_rows"] * row["head_dim"] * 2,
                 "bias": row["retained_rows"] * 4}
        for name, expected in sizes.items():
            actual = uint(artifact[name]["size_bytes"], name + " bytes", positive=True)
            require(actual == expected, name + " file size differs from geometry")
    return coordinates


def geometry_digest(coordinates):
    h = hashlib.sha256(GEOMETRY_DOMAIN)
    h.update(struct.pack("<I", len(coordinates)))
    for row in coordinates:
        coordinate(row)
        h.update(struct.pack("<7I", *(row[name] for name in COORDINATE_FIELDS)))
    return h.digest()


def rounded(value, page):
    result = (value + page - 1) & -page
    return uint(result, "rounded allocation", positive=True)


def validate_manifest_layout(manifest):
    """Map immutable lossy ordinals to physical spans, including exact prefixes/tails."""
    coordinates = manifest_coordinates(manifest)
    owners = sorted({row["owner_layer"] for row in coordinates})
    parts = manifest.get("parts")
    require(type(parts) is list and len(parts) == len(owners) == 3, "owner parts")
    by_key = {(row["owner_layer"], row["head"], row["span"]): row for row in coordinates}
    seen = set()
    destinations = {}
    lengths = []
    for owner, segments in zip(owners, parts):
        require(type(segments) is list and segments, "owner segments")
        logical = 0
        head_count = None
        ordinals = {}
        for span, segment in enumerate(segments):
            count = uint(segment["logical_count"], "segment logical count", 32, True)
            retained = segment.get("retained")
            require(type(retained) is list and retained, "segment retained rows")
            if head_count is None:
                head_count = len(retained)
            require(len(retained) == head_count, "inconsistent segment heads")
            for head, rows in enumerate(retained):
                uint(rows, "segment retained rows", 32, True)
                require(rows <= count, "retained rows exceed source")
                if rows == count:
                    continue
                ordinal = ordinals.get(head, 0)
                key = owner, head, ordinal
                row = by_key.get(key)
                require(row is not None, "missing stored artifact for destination span")
                require((row["logical_start"], row["logical_count"], row["retained_rows"])
                        == (logical, count, rows), "stored coordinate differs from destination span")
                seen.add(key)
                destinations[key] = span
                ordinals[head] = ordinal + 1
            logical = uint(logical + count, "owner logical length", 32, True)
        lengths.append(logical)
    require(len(set(lengths)) == 1, "owner logical lengths differ")
    prompt_tokens = uint(manifest.get("prompt_tokens"), "prompt tokens", 32, True)
    require(all(length == prompt_tokens for length in lengths), "parts do not cover exact prompt")
    require(seen == set(by_key), "unused stored artifacts")
    return [destinations[(row["owner_layer"], row["head"], row["span"])] for row in coordinates]


def validate_budget_receipt(receipt, manifest, *, manifest_path, manifest_sha256,
                            require_declared_match=True, prospective_manifest=None):
    """Return exact native budget fields after checking geometry and arithmetic.

    A historical bad manifest can be probed with require_declared_match=False.
    That mode never validates its admission or changes its immutable contents.
    """
    require(type(require_declared_match) is bool, "declared budget validation mode")
    require(receipt.get("schema_name") == SCHEMA, "budget receipt schema")
    require(receipt.get("completed") is True and receipt.get("diagnostic_only") is True,
            "budget receipt incomplete")
    require(receipt.get("serving_authority") is False and receipt.get("release_authority") is False,
            "budget receipt authority")
    source = receipt.get("input_manifest")
    require(type(source) is dict and set(source) == {"path", "sha256"}, "input manifest edge")
    require(source["path"] == str(manifest_path), "input manifest path")
    require(digest(source["sha256"], "input manifest digest").hex() == manifest_sha256,
            "input manifest digest mismatch")
    coordinates = manifest_coordinates(manifest)
    require(type(receipt.get("artifact_count")) is int and receipt["artifact_count"] == 36,
            "budget artifact count")
    reported = receipt.get("coordinates")
    require(type(reported) is list and len(reported) == 36, "budget coordinates")
    require([coordinate(row) for row in reported] == coordinates, "budget coordinates mismatch")
    require(digest(receipt.get("geometry_sha256"), "geometry digest") == geometry_digest(coordinates),
            "geometry digest mismatch")
    destinations = validate_manifest_layout(manifest)
    reported_destinations = receipt.get("destination_spans")
    require(type(reported_destinations) is list and len(reported_destinations) == 36,
            "destination spans")
    for value in reported_destinations:
        uint(value, "destination span", 32)
    require(receipt.get("layout_validated") is True and reported_destinations == destinations,
            "native destination mapping mismatch")
    if prospective_manifest is not None:
        require(manifest_coordinates(prospective_manifest) == coordinates,
                "budget cannot transfer to different geometry")
        require(validate_manifest_layout(prospective_manifest) == destinations,
                "budget cannot transfer to different destination layout")
    page = uint(receipt.get("host_page_size_bytes"), "page size", positive=True)
    require(4096 <= page <= 1 << 20 and page & (page - 1) == 0, "invalid host page size")
    logical = uint(receipt.get("entries_logical_bytes"), "native entry bytes", positive=True)
    require(logical % len(coordinates) == 0, "native entry array size")
    entries = uint(receipt.get("entries_physical_upper_bytes"), "entry allocation", positive=True)
    require(entries == rounded(logical, page), "entry allocation rounding")
    buffers = sum(rounded(artifact[name]["size_bytes"], page)
                  for artifact in manifest["artifacts"] for name in ("keys", "values", "bias"))
    require(uint(receipt.get("buffers_shared_bytes"), "buffer allocation", positive=True) == buffers,
            "buffer allocation mismatch")
    total = uint(receipt.get("physical_upper_bytes"), "loader allocation", positive=True)
    require(total == entries + buffers, "loader allocation omitted entry array")
    reservation = manifest["reservation"]
    declared_loader = uint(reservation["loader_budget_bytes"], "declared loader", positive=True)
    declared_material = uint(reservation["materialization_host_bytes"], "declared materialization", positive=True)
    for name, expected in (("declared_loader_budget_bytes", declared_loader),
                           ("declared_materialization_host_bytes", declared_material)):
        require(uint(receipt.get(name), name, positive=True) == expected, name + " mismatch")
    loader_matches = total == declared_loader
    material_matches = buffers == declared_material
    require(receipt.get("total_matches_declared_loader_budget") is loader_matches,
            "loader match flag")
    require(receipt.get("buffers_match_declared_materialization") is material_matches,
            "materialization match flag")
    require(material_matches, "materialization budget mismatch")
    if require_declared_match:
        require(loader_matches, "loader budget mismatch")
    if prospective_manifest is not None:
        prospective = prospective_manifest["reservation"]
        require(uint(prospective.get("loader_budget_bytes"), "prospective loader", positive=True) == total
                and uint(prospective.get("materialization_host_bytes"), "prospective materialization", positive=True) == buffers,
                "prospective declared budget mismatch")
    return {"entries_logical_bytes": logical, "entries_physical_upper_bytes": entries,
            "buffers_shared_bytes": buffers, "physical_upper_bytes": total,
            "host_page_size_bytes": page,
            "declared_loader_budget_matches": loader_matches,
            "declared_materialization_budget_matches": material_matches,
            "metadata_admission_validated": require_declared_match and loader_matches,
            "validation_mode": "admission" if require_declared_match else "historical_calculation_only"}


def read_exact_json(path, expected_sha256):
    """Bounded metadata read with duplicate-key and nonfinite-number rejection."""
    path = Path(path)
    require(path.is_absolute() and path.is_file() and not path.is_symlink(), "metadata file")
    require(path.stat().st_size <= 16 << 20, "metadata size limit")
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == expected_sha256, "metadata digest mismatch")

    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("nonfinite JSON number: " + value)

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
