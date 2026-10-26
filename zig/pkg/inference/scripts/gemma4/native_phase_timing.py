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

"""Per-request log attribution; wall intervals are not GPU-event measurements."""

import math

LEGACY_GENERATION_PHASES = (
    "prompt_format",
    "tokenize",
    "multimodal_prepare",
    "runtime_prepare",
    "prefill",
    "decode",
    "text_decode",
)
GENERATION_PHASES = (
    "prompt_format",
    "tokenize",
    "multimodal_prepare",
    "runtime_prepare",
    "prefill",
    "maintenance",
    "decode",
    "text_decode",
)
PREPARE_PHASES = ("query_ns", "snapshot_ns", "fit_ns", "export_write_ns")


def latency_sampling_qualified(profile, warmups, samples, smoke=False):
    minimum = {"bounded": 20, "extended": 100}[profile]
    return not smoke and warmups >= 5 and samples >= minimum


def one_record(text, marker, fields, additional_fields=()):
    matches = [
        line.split(marker, 1)[1].strip() for line in text.splitlines() if marker in line
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one {marker} record, found {len(matches)}")
    result = {}
    for field in matches[0].split():
        key, separator, value = field.partition("=")
        if separator != "=" or key in result or not value:
            raise ValueError("malformed/duplicate timing field")
        result[key] = value
    if set(result) not in (set(fields), set(fields) | set(additional_fields)):
        raise ValueError("missing or unexpected timing fields")
    return result


def integer_fields(record, fields):
    for field in fields:
        value = record[field]
        if not value.isascii() or not value.isdecimal():
            raise ValueError("invalid nonnegative timing integer")
        record[field] = int(value)
    return record


def wall_limit(seconds, units, rounding=0):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("invalid request wall time")
    return seconds * units + rounding


def generation_timing(text, wall_seconds):
    legacy_fields = LEGACY_GENERATION_PHASES + ("total",)
    row = one_record(
        text, "generate_timing_ms: ", legacy_fields, ("maintenance",)
    )
    integer_fields(row, tuple(row))
    # Historical receipts predate the explicit post-prefill maintenance
    # interval. Normalize them without folding current maintenance into decode.
    row.setdefault("maintenance", 0)
    if row["total"] != sum(row[k] for k in GENERATION_PHASES):
        raise ValueError("generation phases do not reconcile")
    if not 0 < row["total"] <= wall_limit(wall_seconds, 1000, rounding=2):
        raise ValueError("generation timing exceeds request wall interval")
    return row


def prepare_timing(
    text, logical_tokens, checkpoint_bytes, wall_seconds, outcome="compacted"
):
    integers = (
        ("source_tokens",)
        + PREPARE_PHASES
        + (
            "export_clock_valid",
            "checkpoint_bytes",
            "retention_retries",
            "terminal_complete",
        )
    )
    savings = ("fitted_savings_ratio", "candidate_savings_ratio")
    row = integer_fields(
        one_record(
            text, "native_checkpoint_prepare_work ", integers + ("outcome",), savings
        ),
        integers,
    )
    # Accept legacy records without either diagnostic, but reject a partial pair.
    # These ratios explain fallback; they do not authorize a quality decision.
    for field in savings:
        if field in row:
            row[field] = float(row[field])
            if not math.isfinite(row[field]) or row[field] <= 0:
                raise ValueError("invalid preparation savings ratio")
    if (
        row["source_tokens"] != logical_tokens
        or row["checkpoint_bytes"] != checkpoint_bytes
        or row["outcome"] != outcome
        or row["terminal_complete"] != 0
    ):
        raise ValueError("preparation timing belongs to a different export")
    if row["export_clock_valid"] != 1 or any(row[k] <= 0 for k in PREPARE_PHASES):
        raise ValueError("missing preparation phase timing")
    total = sum(row[k] for k in PREPARE_PHASES)
    if total > wall_limit(wall_seconds, 1e9):
        raise ValueError("preparation phases exceed request wall interval")
    row["measured_work_sum_ns"] = total
    return row


def request_log(path, offset):
    with path.open("rb") as source:
        if source.seek(0, 2) < offset:
            raise ValueError("worker log truncated during request")
        source.seek(offset)
        return source.read().decode("utf-8")
