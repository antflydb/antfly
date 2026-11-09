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

"""Compare benchmark SQL payloads independently of generated storage row IDs.

Callers supply declared SQL primary-key columns and apply any explicit fixture
normalization before adding documents. Only identities and value hashes are
retained; large payloads do not remain in memory or appear in the report.
"""

import hashlib
import json


def _encode(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _fold(pairs):
    digest = hashlib.sha256()
    for identity, value in sorted(pairs):
        digest.update(len(identity).to_bytes(8, "little"))
        digest.update(identity)
        digest.update(value)
    return digest.hexdigest()


class LogicalFieldHashes:
    def __init__(self, key_columns):
        self.key_columns = tuple(key_columns)
        if not self.key_columns or len(set(self.key_columns)) != len(self.key_columns):
            raise ValueError("distinct logical primary-key columns are required")
        self._identities = set()
        self._fields = {}

    def add(self, document):
        try:
            key = [document[column] for column in self.key_columns]
        except KeyError as error:
            raise ValueError("missing logical primary-key column") from error
        if any(value is None for value in key):
            raise ValueError("null logical primary key")
        identity = _encode(key)
        if identity in self._identities:
            raise ValueError("duplicate logical primary key")
        self._identities.add(identity)
        for field, value in document.items():
            self._fields.setdefault(field, []).append(
                (identity, hashlib.sha256(_encode(value)).digest())
            )

    def report(self):
        return {
            "rows": len(self._identities),
            "logical_key_columns": list(self.key_columns),
            "logical_row_sha256": _fold((key, b"") for key in self._identities),
            "field_sha256": {
                field: _fold(values) for field, values in self._fields.items()
            },
        }
