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

"""Fault injection for the independent distributed alias result oracle."""

import pytest

from index_alias_qualification import EXPECTED, validate_result


@pytest.mark.parametrize("target", sorted(EXPECTED))
@pytest.mark.parametrize("shard", ["0", "8", "z"])
def test_alias_oracle_rejects_each_stale_shard(target, shard):
    other = next(name for name in EXPECTED if name != target)
    ids = [key for key in EXPECTED[target] if not key.startswith(shard + ":")]
    ids.extend(key for key in EXPECTED[other] if key.startswith(shard + ":"))
    value = {
        "resolved_indexes": [target],
        "hits": {"hits": [{"_id": key} for key in ids]},
    }
    with pytest.raises(AssertionError):
        validate_result(value)


@pytest.mark.parametrize("target", sorted(EXPECTED))
def test_alias_oracle_accepts_complete_target(target):
    value = {
        "resolved_indexes": [target],
        "hits": {"hits": [{"_id": key} for key in EXPECTED[target]]},
    }
    assert validate_result(value) == target
