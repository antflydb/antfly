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

"""Small independent reference functions; no engine code or SQL rewriting.

SQLite carries JSON as encoded text. These helpers cover explicitly selected
scalar inputs, not a general PostgreSQL type system or nested JSON constructor
provenance. Public/native evidence remains required for every original case.
"""

import json


def register(db):
    def json_type(value):
        if value is None:
            return None
        value = json.loads(value)
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, str):
            return "string"
        return "array" if isinstance(value, list) else "object"

    def extract(value, *keys):
        if value is None or any(key is None for key in keys):
            return None
        value = json.loads(value)
        for key in keys:
            if isinstance(value, dict):
                value = value.get(str(key))
            elif isinstance(value, list):
                try:
                    value = value[int(key)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        if value is None:
            return None
        return (
            value
            if isinstance(value, str)
            else json.dumps(value, separators=(",", ":"))
        )

    def build_object(*args):
        if len(args) % 2 or any(key is None for key in args[::2]):
            raise ValueError("JSON object requires non-null scalar key/value pairs")
        return json.dumps(
            dict(zip(map(str, args[::2]), args[1::2], strict=True)),
            separators=(",", ":"),
        )

    db.create_function(
        "strpos",
        2,
        lambda text, needle: (
            None if text is None or needle is None else text.find(needle) + 1
        ),
        deterministic=True,
    )
    db.create_function(
        "bit_length",
        1,
        lambda text: None if text is None else len(text.encode("utf-8")) * 8,
        deterministic=True,
    )
    db.create_function(
        "ends_with",
        2,
        lambda text, suffix: (
            None if text is None or suffix is None else text.endswith(suffix)
        ),
        deterministic=True,
    )
    db.create_function(
        "to_jsonb",
        1,
        lambda value: (
            None if value is None else json.dumps(value, separators=(",", ":"))
        ),
        deterministic=True,
    )
    db.create_function("jsonb_typeof", 1, json_type, deterministic=True)
    db.create_function("jsonb_extract_path_text", -1, extract, deterministic=True)
    db.create_function("jsonb_build_object", -1, build_object, deterministic=True)
