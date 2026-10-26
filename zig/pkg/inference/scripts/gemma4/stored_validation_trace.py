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

"""Validate stored-artifact diagnostic reports without reading tensor payloads.

The native float32 bit patterns are authoritative. Margins are calculated in
Python float64 for reporting only; this module grants no execution or release
authority and never changes the native acceptance thresholds.
"""

import math
import struct


SCHEMA = "antfly.context.stored-validation-trace.v1"
CAPACITY = 36
REPORT_BYTES = 32
COORDINATE_FIELDS = {
    "owner_layer", "head", "span", "logical_start", "logical_count",
    "retained_rows", "head_dim",
}
METRICS = {
    "training_mass_relative_max_error": (1, "mass"),
    "mass_relative_max_error": (2, "mass"),
    "output_relative_rmse": (4, "output"),
    "max_consumer_output_relative_rmse": (4, "output"),
}
REPORT_FIELDS = {
    "failure_flags", "iterations", "evaluated", "validation_groups",
    *METRICS, *(name + "_bits" for name in METRICS),
}
ENTRY_FIELDS = {
    "coordinate", "disposition", "accepted", "failure", "report_present",
    "report", "mass_tolerance_bits", "output_tolerance_bits",
    "mass_tolerance", "output_tolerance", "expected_validation_groups",
}
FAILURES = {"none", "non_finite_input", "quality_rejected", "invalid_report", "other"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def u32(value, label):
    require(type(value) is int and 0 <= value <= 0xFFFFFFFF, label + " must be u32")
    return value


def float32_bits(value, label):
    require(type(value) in (int, float) and math.isfinite(value), label + " must be finite")
    try:
        return struct.unpack("<I", struct.pack("<f", value))[0]
    except (OverflowError, struct.error) as exc:
        raise ValueError(label + " exceeds float32") from exc


def decode_float32(bits, label):
    return struct.unpack("<f", struct.pack("<I", u32(bits, label)))[0]


def check_mirror(bits, mirror, label):
    value = decode_float32(bits, label)
    if math.isfinite(value):
        require(float32_bits(mirror, label + " mirror") == bits, label + " mirror differs")
    else:
        require(mirror is None, label + " nonfinite mirror must be null")
    return value


def unused_entry():
    return {
        "coordinate": dict.fromkeys(COORDINATE_FIELDS, 0),
        "disposition": "unused", "accepted": False, "failure": "none",
        "report_present": False,
        "report": {
            "failure_flags": 0, "iterations": 0, "evaluated": 0, "validation_groups": 0,
            **{name + "_bits": 0 for name in METRICS},
            **dict.fromkeys(METRICS),
        },
        "mass_tolerance_bits": 0, "output_tolerance_bits": 0,
        "mass_tolerance": None, "output_tolerance": None,
        "expected_validation_groups": 0,
    }


def check_coordinate(value, label):
    require(type(value) is dict and set(value) == COORDINATE_FIELDS, label + " coordinate fields")
    for key, number in value.items():
        u32(number, label + " " + key)


def validate_trace(trace, coordinates, *, mass_tolerance, output_tolerance,
                   expected_validation_groups, status_readback_bytes, require_success=False):
    """Check a bounded native trace against its reservation and readback counter.

    A valid failure trace reports completed work and grants no numerical pass.
    Even a complete numerical pass is separate from publication, allocation,
    model quality, serving, and release qualification.
    """
    require(type(require_success) is bool, "require_success must be boolean")
    require(type(trace) is dict and set(trace) == {
        "schema_name", "attempted_count", "report_count", "accepted_count", "overflowed", "entries",
    }, "trace fields")
    require(trace["schema_name"] == SCHEMA and trace["overflowed"] is False, "trace schema or overflow")
    require(type(coordinates) is list and 0 < len(coordinates) <= CAPACITY, "reservation coordinates")
    for index, coordinate in enumerate(coordinates):
        check_coordinate(coordinate, "reservation " + str(index))
    require(type(expected_validation_groups) is list and len(expected_validation_groups) == len(coordinates),
            "expected validation group counts")
    for groups in expected_validation_groups:
        require(u32(groups, "expected validation groups") > 0, "expected validation groups must be positive")
    require(len({tuple(c[k] for k in sorted(COORDINATE_FIELDS)) for c in coordinates}) == len(coordinates),
            "duplicate reservation coordinate")
    attempted, reported, accepted = (u32(trace[k], k) for k in (
        "attempted_count", "report_count", "accepted_count"))
    require(accepted <= reported <= attempted <= len(coordinates), "trace counts")
    require(type(status_readback_bytes) is int and status_readback_bytes == reported * REPORT_BYTES,
            "readback bytes differ from reports")
    entries = trace["entries"]
    require(type(entries) is list and len(entries) == CAPACITY, "fixed trace capacity")
    expected_bits = {
        "mass": float32_bits(mass_tolerance, "mass tolerance"),
        "output": float32_bits(output_tolerance, "output tolerance"),
    }
    for name, bits in expected_bits.items():
        require(decode_float32(bits, name) > 0, name + " tolerance must be positive")
    seen_reported = seen_accepted = 0
    result = []
    for index, entry in enumerate(entries):
        require(type(entry) is dict and set(entry) == ENTRY_FIELDS, "entry fields")
        check_coordinate(entry["coordinate"], "entry " + str(index))
        require(type(entry["accepted"]) is bool and type(entry["report_present"]) is bool, "entry booleans")
        report = entry["report"]
        require(type(report) is dict and set(report) == REPORT_FIELDS, "report fields")
        for key in ("failure_flags", "iterations", "evaluated", "validation_groups"):
            u32(report[key], key)
        for name in METRICS:
            u32(report[name + "_bits"], name + " bits")
        for name in expected_bits:
            u32(entry[name + "_tolerance_bits"], name + " tolerance bits")
        u32(entry["expected_validation_groups"], "entry expected validation groups")
        if index >= attempted:
            require(entry == unused_entry(), "nonempty unused entry")
            continue
        require(entry["coordinate"] == coordinates[index], "trace coordinate order differs")
        require(entry["expected_validation_groups"] == expected_validation_groups[index],
                "entry expected validation groups differ from reservation")
        require(type(entry["failure"]) is str and entry["failure"] in FAILURES, "unknown failure")
        tolerances = {}
        for name, bits in expected_bits.items():
            require(entry[name + "_tolerance_bits"] == bits, name + " tolerance changed")
            tolerances[name] = check_mirror(bits, entry[name + "_tolerance"], name + " tolerance")
        present = entry["report_present"]
        if entry["accepted"]:
            require(present and entry["disposition"] == "accepted" and entry["failure"] == "none",
                    "accepted disposition")
            seen_accepted += 1
        else:
            require(index == attempted - 1 and entry["failure"] != "none", "failure must terminate trace")
            require(entry["disposition"] == ("rejected_with_report" if present else "failed_before_report"),
                    "failure disposition")
        metrics = {}
        if present:
            seen_reported += 1
            require(report["evaluated"] & ~7 == 0, "unknown evaluated bits")
            for name, (mask, tolerance_name) in METRICS.items():
                observed = check_mirror(report[name + "_bits"], report[name], name)
                evaluated = bool(report["evaluated"] & mask)
                finite = math.isfinite(observed)
                metrics[name] = {
                    "bits": report[name + "_bits"], "evaluated": evaluated,
                    "finite": finite, "observed": observed if evaluated and finite else None,
                    "threshold": tolerances[tolerance_name],
                    "margin": tolerances[tolerance_name] - observed if evaluated and finite else None,
                }
            valid_status = (report["iterations"] == 0 and report["evaluated"] == 7 and
                            report["validation_groups"] == expected_validation_groups[index])
            valid_metrics = all(m["finite"] and m["observed"] is not None and m["margin"] >= 0
                                for m in metrics.values())
            if entry["accepted"]:
                require(report["failure_flags"] == 0 and valid_status, "accepted report status")
                require(valid_metrics, "accepted report exceeds thresholds")
            elif entry["failure"] == "quality_rejected":
                require(report["failure_flags"] != 0 and report["failure_flags"] & 1 == 0,
                        "quality rejection flags")
            elif entry["failure"] == "non_finite_input":
                require(report["failure_flags"] & 1 != 0, "nonfinite rejection flags")
            elif entry["failure"] == "invalid_report":
                require(report["failure_flags"] == 0 and not (valid_status and valid_metrics),
                        "invalid-report disposition contradicts native predicates")
        else:
            require(report == unused_entry()["report"], "absent report contains stale data")
        result.append({
            "coordinate": dict(entry["coordinate"]), "accepted": entry["accepted"],
            "disposition": entry["disposition"], "failure": entry["failure"],
            "report_present": present, "metrics": metrics,
        })
    require((seen_reported, seen_accepted) == (reported, accepted), "trace counters disagree")
    complete = attempted == reported == accepted == len(coordinates)
    if require_success:
        require(complete, "incomplete numerical success")
    return {
        "schema_name": "antfly.context.stored-validation-summary.v1",
        "trace_valid": True, "all_numerical_reports_accepted": complete,
        "attempted_count": attempted, "report_count": reported, "accepted_count": accepted,
        "status_readback_bytes": status_readback_bytes, "entries": result,
        "execution_authority": False, "publication_authority": False,
        "serving_authority": False, "release_authority": False,
    }
