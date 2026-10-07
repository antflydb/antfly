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

"""Explicit compatibility exception for Fastino's synthetic terminal period.

Coordinates use Unicode code points. Only entity predictions and relations
whose spans end in the single appended period are excluded. Other invalid
coordinates, record differences and valid-source predictions remain errors.
The original captures are never modified.
"""

import copy


def validate_source_spans(text, value):
    if isinstance(value, dict):
        source = value.get("source")
        if source is not None:
            start, end = source.get("start"), source.get("end")
            if (
                type(start) is not int
                or type(end) is not int
                or not 0 <= start < end <= len(text)
            ):
                raise ValueError("prediction lies outside original source text")
        for child in value.values():
            validate_source_spans(text, child)
    elif isinstance(value, list):
        for child in value:
            validate_source_spans(text, child)


def original_text_reference(text, output):
    result = copy.deepcopy(output)
    excluded = []

    def synthetic(value):
        source = value.get("source")
        if not isinstance(source, dict):
            return False
        start, end = source.get("start"), source.get("end")
        return (
            type(start) is int
            and type(end) is int
            and 0 <= start <= len(text)
            and end == len(text) + 1
            and value.get("text") == (text + ".")[start:end]
        )

    for group_index, entity in enumerate(result["entities"]):
        kept = []
        for index, value in enumerate(entity["values"]):
            if synthetic(value):
                excluded.append(
                    dict(
                        path=f"entities[{group_index}].values[{index}]",
                        prediction=value,
                    )
                )
            else:
                kept.append(value)
        entity["values"] = kept
    kept = []
    for index, relation in enumerate(result["relations"]):
        head_synthetic, tail_synthetic = (
            synthetic(relation["head"]),
            synthetic(relation["tail"]),
        )
        if head_synthetic or tail_synthetic:
            if not head_synthetic:
                validate_source_spans(text, relation["head"])
            if not tail_synthetic:
                validate_source_spans(text, relation["tail"])
            excluded.append(dict(path=f"relations[{index}]", prediction=relation))
        else:
            kept.append(relation)
    result["relations"] = kept
    validate_source_spans(text, result)
    return result, excluded
