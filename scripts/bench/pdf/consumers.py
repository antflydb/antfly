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

"""Independent PDF enrichment consumers and completion gates for sharing probes."""

import copy
from urllib.parse import quote


def consumer_name(name, index):
    return name if index == 0 else f"{name}_consumer{index + 1}"


def table_with_consumers(config, count):
    if count not in (1, 2):
        raise ValueError("consumer count must be one or two")
    result = copy.deepcopy(config)
    indexes = config["indexes"]
    identifiers = list(indexes) + [
        "document_units_v1",
        "document_chunks_v1",
        "document_chunk_dense_v1",
    ]
    for index in range(1, count):
        names = {name: consumer_name(name, index) for name in identifiers}

        def rename(value, names=names):
            if isinstance(value, str):
                return names.get(value, value)
            if isinstance(value, dict):
                return {names.get(k, k): rename(v) for k, v in value.items()}
            if isinstance(value, list):
                return [rename(v) for v in value]
            return value

        result["indexes"].update(rename(indexes))
    return result


def collect_manifests(api, table_url, selected, count):
    manifests = {}
    for index in range(count):
        name = consumer_name("document_units_v1", index)
        current = {}
        manifests[name] = current
        for row in selected:
            url = f"{table_url}/documents/{quote(row['path'], safe='')}/artifacts/{name}?detail=raw"
            try:
                current[row["path"]] = api.json_request("GET", url)
            except (RuntimeError, OSError) as exc:
                current[row["path"]] = {"lookup_error": repr(exc)}
    return manifests


def all_consumers_complete(api, statuses, manifests, documents, count):
    by_name = api._index_statuses(statuses)
    for index in range(count):
        name = consumer_name("document_units_v1", index)
        if len(manifests.get(name, {})) != documents or not all(
            value.get("merge_status") == "converged"
            for value in manifests[name].values()
        ):
            return False
        coverage = by_name.get(consumer_name("document_vectors", index), {}).get(
            "coverage", {}
        )
        if not (
            coverage.get("observation_complete") is True
            and coverage.get("complete") is True
            and coverage.get("source_total") == documents
        ):
            return False
    return True
