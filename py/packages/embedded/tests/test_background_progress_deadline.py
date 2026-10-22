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

from conftest import BackgroundProgressDeadline


def test_idle_deadline_ignores_attempts_targets_and_regressing_views():
    now = [0.0]
    deadline = BackgroundProgressDeadline(clock=lambda: now[0])
    assert not deadline.expired({"enrichment": {"applied_sequence": 5}})
    now[0] = 20
    assert not deadline.expired({"enrichment": {"applied_sequence": 4, "target_sequence": 100}})
    now[0] = 30
    assert deadline.expired(
        {
            "enrichment": {"applied_sequence": 5, "target_sequence": 200},
            "background_publications": {"attempts": 10000},
        }
    )


def test_durable_stage_and_replacement_progress_extend_only_idle_deadline():
    now = [0.0]
    deadline = BackgroundProgressDeadline(clock=lambda: now[0])
    for second, stats in [
        (25, {"resolution": {"applied_sequence": 1}}),
        (50, {"promotion": {"applied_sequence": 1}}),
        (75, {"replacements": [{"name": "kg2", "scanned_keys": 100}]}),
        (100, {"replacements": [{"name": "kg2", "applied_sequence": 2}]}),
    ]:
        now[0] = second
        assert not deadline.expired(stats)
    now[0] = 120
    assert deadline.expired({"promotion": {"applied_sequence": 100}})


def test_completed_results_advance_idle_deadline_without_allowing_oscillation():
    now = [0.0]
    deadline = BackgroundProgressDeadline(clock=lambda: now[0])
    now[0] = 25
    assert not deadline.expired({}, completed=1)
    now[0] = 50
    assert not deadline.expired({}, completed=0)
    now[0] = 55
    assert deadline.expired({}, completed=1)
