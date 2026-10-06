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

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class LookupState(Enum):
    MISSING = "missing"
    PRESENT = "present"


class RegistryError(RuntimeError):
    """A registry could not provide an authoritative answer."""


@dataclass(frozen=True)
class Lookup:
    state: LookupState
    digest: str | None = None

    @classmethod
    def missing(cls) -> "Lookup":
        return cls(LookupState.MISSING)

    @classmethod
    def present(cls, digest: str) -> "Lookup":
        return cls(LookupState.PRESENT, digest)
