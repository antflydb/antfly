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

"""Bind independent captures to the complete reviewed upstream Python tree."""
import hashlib

SOURCE_SHA256 = "70f7b5084c98a420558be740b726de8ef86edb2882f93cba8af6946db914e6d4"


def verify_source(directory):
    digest = hashlib.sha256()
    for path in sorted((directory / "gliner2").rglob("*.py")):
        digest.update(str(path.relative_to(directory)).encode())
        digest.update(b"\0" + path.read_bytes() + b"\0")
    if digest.hexdigest() != SOURCE_SHA256:
        raise ValueError("upstream GLiNER2 source differs from the pinned revision")
    return SOURCE_SHA256
