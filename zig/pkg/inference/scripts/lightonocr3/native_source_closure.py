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

"""Keep relocated native validation sources independent of local include paths."""

import hashlib
import re
from pathlib import Path

_INCLUDE = re.compile(r'^\s*#include\s+"([^"]+)"\s*$', re.MULTILINE)


def expand_native_source(path, stack=()):
    path = Path(path).resolve()
    if path in stack:
        return ""  # The enclosing owner guard already protects this cycle.
    source = path.read_text()
    source = re.sub(r"^\s*#pragma once\s*$", "", source, flags=re.MULTILINE)

    def include(match):
        owner = (path.parent / match[1]).resolve()
        if not owner.is_file():
            return match[0]  # Non-local compiler/toolkit include.
        guard = (
            "ANTFLY_VALIDATION_OWNER_"
            + hashlib.sha256(str(owner).encode()).hexdigest()[:24]
        )
        body = expand_native_source(owner, (*stack, path))
        # Native guards preserve conditional-include behavior and diamond graphs.
        return f"#ifndef {guard}\n#define {guard}\n{body}\n#endif\n"

    return _INCLUDE.sub(include, source)
