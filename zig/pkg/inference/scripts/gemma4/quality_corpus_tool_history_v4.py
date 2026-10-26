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

"""Derive genuine historical tool-response blueprints without model outputs."""
import copy
import hashlib
import json


def require(value, message):
    if not value:
        raise ValueError(message)


def _strict(raw):
    def pairs(rows):
        out = {}
        for key, value in rows:
            require(key not in out, "duplicate JSON field: " + key)
            out[key] = value
        return out
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def derive(raw):
    source = _strict(raw)
    require(type(source) is dict and source.get("schema_version") == 2 and
            source.get("model_outputs_observed") is False and type(source.get("cases")) is list,
            "invalid frozen source blueprint")
    result = copy.deepcopy(source)
    result["schema_version"] = 3
    result["stage"] = "quality_source_task_blueprints_historical_tool_response_v1"
    result["parent_blueprints_sha256"] = hashlib.sha256(raw).hexdigest()
    count = 0
    for case in result["cases"]:
        if case.get("bucket") != "structured_tool":
            continue
        require(type(case.get("id")) is str and case["id"] and
                type(case.get("question_utf8")) is str and case["question_utf8"] and
                type(case.get("source")) is dict,
                "invalid structured-tool blueprint")
        source_row = case["source"]
        require(type(source_row.get("path")) is str and source_row["path"] and
                type(source_row.get("sha256")) is str and len(source_row["sha256"]) == 64 and
                type(source_row.get("byte_start")) is int and type(source_row.get("byte_end")) is int and
                0 <= source_row["byte_start"] < source_row["byte_end"],
                "invalid structured-tool source range")
        call_id = "call_" + hashlib.sha256(case["id"].encode()).hexdigest()[:24]
        case["tool_history"] = {
            "schema_version": 1,
            "initial_request_utf8": "Retrieve the cited source range with read_source_ranges.",
            "assistant_call": {
                "id": call_id,
                "type": "function",
                "function": {
                    "name": "read_source_ranges",
                    "arguments_contract": "packed_selected_source_ranges_v1",
                },
            },
            "tool_call_id": call_id,
            "final_request_utf8": case["question_utf8"],
        }
        count += 1
    require(count == 100, "expected exactly 100 structured-tool blueprints")
    return result
