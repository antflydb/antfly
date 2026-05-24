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

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml


class OpenApiLoader(yaml.SafeLoader):
    yaml_implicit_resolvers = {
        key: list(resolvers)
        for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


for key, resolvers in list(OpenApiLoader.yaml_implicit_resolvers.items()):
    OpenApiLoader.yaml_implicit_resolvers[key] = [
        (tag, pattern)
        for tag, pattern in resolvers
        if tag != "tag:yaml.org,2002:timestamp"
    ]


def normalize(value: object) -> None:
    if isinstance(value, dict):
        for key, child in list(value.items()):
            if str(key) == "description" and isinstance(child, str):
                value[key] = " ".join(child.split())
            else:
                normalize(child)
        return
    if isinstance(value, list):
        for child in value:
            normalize(child)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("usage: yaml_to_json.py <input.yaml> <output.json>")

    input_path = Path(argv[0])
    output_path = Path(argv[1])
    with input_path.open("r", encoding="utf-8") as fh:
        data = yaml.load(fh, Loader=OpenApiLoader)
    normalize(data)
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
