#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Deterministic phase-value -> color assignment shared across visualization
layers.

A model's phase domain (the string values of a variable like splitPhase or
status, in spec declaration order) is mapped onto a fixed categorical palette,
so e.g. "splitting" gets the same hue in the generated Mermaid state diagram
and in a trace timeline's tenure bands. The palette order is CVD-validated;
domains larger than the palette are left uncolored rather than recycled.
"""

from __future__ import annotations

from pathlib import Path

# Categorical palette slots (light-surface hex), CVD-safe in this order.
SLOTS = [
    "#2a78d6",  # blue
    "#eb6834",  # orange
    "#1baf7a",  # aqua
    "#eda100",  # yellow
    "#e87ba4",  # magenta
    "#008300",  # green
    "#4a3aa7",  # violet
    "#e34948",  # red
]

# Reserved status color for fault/crash events; never used for phases.
FAULT_COLOR = "#d03b3b"


def assign(values: list[str]) -> dict[str, str]:
    """Map domain values (in declaration order) to palette hexes. Empty when
    the domain exceeds the palette (no recycled or invented hues)."""
    values = list(dict.fromkeys(values))
    if not values or len(values) > len(SLOTS):
        return {}
    return {v: SLOTS[i] for i, v in enumerate(values)}


def model_phase_colors(specs_dir: Path, model: str, phase_var: str) -> dict[str, str]:
    """Colors for a model's phase variable, e.g.
    model_phase_colors(specs, "metadata/AntflySplitRefinementBridge", "phase").
    Returns {} if the spec or variable cannot be resolved."""
    import gen_structural  # deferred: pulls in tree-sitter

    path = specs_dir / f"{model}.tla"
    if not path.is_file():
        return {}
    try:
        spec = gen_structural.Spec(path)
    except Exception:
        return {}
    domain = spec.phase_domains.get(phase_var)
    return assign(domain) if domain else {}


def wash(hex_color: str, alpha: str = "30") -> str:
    """Translucent band fill (8-digit hex) that reads on both light and dark
    surfaces."""
    return hex_color + alpha
