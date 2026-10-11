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

"""Read-your-writes requirements shared by query and SQL requests."""

from collections.abc import Mapping
from typing import Any

from .client_generated.models import ReadReceipt, ReadRequirements, ReadRequirementsVisibility


def read_after_write(receipt: ReadReceipt | Mapping[str, Any], wait_ms: int = 5000) -> ReadRequirements:
    """Pass an ingestion receipt back unchanged; do not construct WAL coordinates."""
    if not isinstance(receipt, ReadReceipt):
        receipt = ReadReceipt.from_dict(receipt)
    return ReadRequirements(
        visibility=ReadRequirementsVisibility.ACCEPTED,
        after_write=receipt,
        wait_ms=wait_ms,
    )
