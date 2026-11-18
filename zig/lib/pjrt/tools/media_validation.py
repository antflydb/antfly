# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Validate saved CPU wrapper oracles before using them for qualification."""

import math

import numpy as np


def validate_oracle(oracle, case_names, max_abs):
    """Return upstream vectors only after validating every saved wrapper result."""
    rows = oracle["cases"]
    names = list(case_names)
    if (
        not names
        or len(set(names)) != len(names)
        or len(rows) != len(names)
        or {row["name"] for row in rows} != set(names)
    ):
        raise ValueError("Incomplete or duplicate CPU oracle cases")
    if not math.isfinite(max_abs) or max_abs < 0:
        raise ValueError("Invalid CPU oracle tolerance")
    vectors = {}
    for row in rows:
        error = row["max_abs"]
        if (
            not math.isfinite(error)
            or error < 0
            or error > max_abs
            or not math.isfinite(row["cosine"])
        ):
            raise ValueError(
                "CPU oracle wrapper error is nonfinite or exceeds tolerance"
            )
        upstream = np.asarray(row["upstream_vector"], dtype=np.float64)
        wrapper = np.asarray(row["wrapper_vector"], dtype=np.float64)
        for vector in (upstream, wrapper):
            if (
                vector.shape != (768,)
                or not np.isfinite(vector).all()
                or abs(np.linalg.norm(vector) - 1) > 0.001
            ):
                raise ValueError("Invalid CPU oracle vector")
        # A success-looking summary must not hide a failed wrapper vector.
        if np.max(np.abs(wrapper - upstream)) > max_abs:
            raise ValueError("CPU oracle wrapper vector exceeds tolerance")
        vectors[row["name"]] = row["upstream_vector"]
    return vectors
